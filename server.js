'use strict';
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');
const axios = require('axios');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

const PORT = process.env.PORT || 3000;
const DOWNLOAD_DIR = path.join(__dirname, 'downloads');
const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const GITHUB_OWNER = process.env.GITHUB_OWNER;
const GITHUB_REPO = process.env.GITHUB_REPO;
const GITHUB_BRANCH = process.env.GITHUB_BRANCH || 'main';

// ─── Persistence files ─────────────────────────────────────────────────────

const DOWNLOADED_IDS_FILE = path.join(__dirname, 'downloaded_ids.json');
const PLAYLIST_INFO_FILE = path.join(__dirname, 'playlist_info.json');

// ─── Deduplication & playlist tracking ──────────────────────────────────────

function loadIds() {
  try {
    if (fs.existsSync(DOWNLOADED_IDS_FILE)) {
      return new Set(JSON.parse(fs.readFileSync(DOWNLOADED_IDS_FILE, 'utf8')));
    }
  } catch {}
  return new Set();
}

function saveIds(ids) {
  fs.writeFileSync(DOWNLOADED_IDS_FILE, JSON.stringify([...ids], null, 2));
}

function loadPlaylistInfo() {
  try {
    if (fs.existsSync(PLAYLIST_INFO_FILE)) {
      return JSON.parse(fs.readFileSync(PLAYLIST_INFO_FILE, 'utf8'));
    }
  } catch {}
  return {}; // { videoId: { name, url, playlistId } }
}

function savePlaylistInfo(info) {
  fs.writeFileSync(PLAYLIST_INFO_FILE, JSON.stringify(info, null, 2));
}

let downloadedIds = loadIds();
let playlistInfo = loadPlaylistInfo();

function getVideoId(url) {
  const m = url.match(/(?:v=|\/shorts\/|youtu\.be\/)([a-zA-Z0-9_-]{11})/);
  return m ? m[1] : null;
}

function isPlaylistUrl(url) {
  return /[?&]list=/.test(url) && !/[?&]v=/.test(url) && !/\/shorts\//.test(url);
}

// ─── SSE ──────────────────────────────────────────────────────────────────

const sseClients = new Set();

function sseSend(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) {
    try { res.write(payload); } catch {}
  }
}

// ─── GitHub API ────────────────────────────────────────────────────────────

const GH_HEADERS = {
  Authorization: `Bearer ${GITHUB_TOKEN}`,
  Accept: 'application/vnd.github.v3+json',
  'X-GitHub-Api-Version': '2022-11-28'
};

async function ghGet(path) {
  const r = await axios.get(`https://api.github.com${path}`, { headers: GH_HEADERS });
  return r.data;
}

async function ghGetContents(filePath) {
  try {
    return await ghGet(`/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/${filePath}?ref=${GITHUB_BRANCH}`);
  } catch (e) {
    if (e.response?.status === 404) return null;
    throw e;
  }
}

async function ghUploadFile(content, filePath, message) {
  const data = Buffer.from(content).toString('base64');
  const existing = await ghGetContents(filePath);
  const payload = { message, content: data, branch: GITHUB_BRANCH };
  if (existing?.sha) payload.sha = existing.sha;
  const r = await axios.put(
    `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/${filePath}`,
    payload, { headers: GH_HEADERS }
  );
  return r.data.content?.sha ? 'updated' : 'created';
}

async function ghDeleteFile(filePath, sha, message) {
  await axios.delete(
    `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/${filePath}`,
    { data: { message, sha, branch: GITHUB_BRANCH }, headers: GH_HEADERS }
  );
}

async function ghGetAllMp3s() {
  const files = [];
  let page = 1;
  while (true) {
    const tree = await ghGet(`/repos/${GITHUB_OWNER}/${GITHUB_REPO}/git/trees/${GITHUB_BRANCH}?recursive=1&page=${page}`);
    if (!tree.tree) break;
    files.push(...tree.tree.filter(f => f.path.endsWith('.mp3')));
    if (tree.tree.length < 100) break;
    page++;
  }
  return files;
}

// ─── yt-dlp ────────────────────────────────────────────────────────────────

function ytArgs() {
  return [
    '--newline', '--no-playlist', '--no-update',
    '-x', '--audio-format', 'mp3', '--audio-quality', '0',
    '--embed-metadata', '--no-progress',
    '--extractor-args', 'youtube:player_client=web,default'
  ];
}

async function downloadSingle(url, videoId) {
  return new Promise((resolve, reject) => {
    const outputTemplate = path.join(DOWNLOAD_DIR, '%(id)s_%(title)s.%(ext)s');
    const proc = spawn('yt-dlp', [...ytArgs(), '-o', outputTemplate, url], { shell: true });
    let stderr = '';
    proc.stderr.on('data', d => { stderr += d.toString(); });
    proc.on('close', code => {
      if (code !== 0) return reject(new Error(stderr || `yt-dlp exited ${code}`));
      const files = fs.readdirSync(DOWNLOAD_DIR)
        .filter(f => f.startsWith(videoId) && f.endsWith('.mp3'));
      resolve(files[0] || null);
    });
    proc.on('error', reject);
  });
}

async function downloadAndUpload(url, videoId, playlistRef) {
  sseSend('download-start', { videoId });
  try {
    const filename = await downloadSingle(url, videoId);
    if (!filename) throw new Error('Download completed but file not found');
    const content = fs.readFileSync(path.join(DOWNLOAD_DIR, filename));
    const status = await ghUploadFile(content, filename, `Add: ${filename}`);
    sseSend('download-done', { videoId, filename, status, playlistRef });
    return { success: true, filename, status };
  } catch (e) {
    sseSend('download-error', { videoId, error: e.message });
    throw e;
  }
}

// ─── Routes ────────────────────────────────────────────────────────────────

app.get('/api/status', (req, res) => {
  res.json({ ok: true, downloadedCount: downloadedIds.size });
});

app.get('/api/sse', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  res.write(`event: connected\ndata: ${JSON.stringify({ downloadedCount: downloadedIds.size })}\n\n`);
  sseClients.add(res);
  req.on('close', () => { sseClients.delete(res); res.end(); });
});

app.post('/api/download', async (req, res) => {
  let { url } = req.body;
  if (!url) return res.status(400).json({ error: 'URL required' });

  // Normalize music.youtube.com
  url = url.replace(/^https?:\/\/music\.youtube\.com/, 'https://www.youtube.com');

  if (isPlaylistUrl(url)) {
    // ── PLAYLIST ──
    let infoData;
    try {
      sseSend('playlist-start', { url });
      infoData = await new Promise((resolve, reject) => {
        const p = spawn('yt-dlp', [
          '--flat-playlist', '-J', '--no-warnings',
          '--extractor-args', 'youtube:player_client=web,default', url
        ], { shell: true });
        let d = '';
        p.stdout.on('data', x => d += x.toString());
        p.stderr.on('data', () => {});
        p.on('close', code => {
          if (d.trim()) resolve(d);
          else reject(new Error(d || `yt-dlp exited ${code}`));
        });
        p.on('error', reject);
      });
    } catch (e) {
      return res.status(500).json({ error: `Playlist info: ${e.message}` });
    }

    const playlist = JSON.parse(infoData);
    const entries = (playlist.entries || []).filter(Boolean);
    const playlistId = playlist.id || url;
    const playlistName = playlist.title || 'Playlist';
    const results = [];

    for (const entry of entries) {
      const vid = entry.id;
      if (downloadedIds.has(vid)) {
        results.push({ videoId: vid, title: entry.title, status: 'skipped' });
        sseSend('download-skipped', { videoId: vid, title: entry.title });
        continue;
      }
      try {
        const r = await downloadAndUpload(
          `https://www.youtube.com/watch?v=${vid}`, vid,
          { id: playlistId, name: playlistName }
        );
        downloadedIds.add(vid);
        playlistInfo[vid] = { name: playlistName, url, playlistId };
        saveIds(downloadedIds);
        savePlaylistInfo(playlistInfo);
        results.push({ videoId: vid, title: entry.title, status: 'done', filename: r.filename });
      } catch (e) {
        results.push({ videoId: vid, title: entry.title, status: 'error', error: e.message });
        sseSend('download-error', { videoId: vid, error: e.message });
      }
    }

    saveIds(downloadedIds);
    savePlaylistInfo(playlistInfo);
    sseSend('playlist-done', { playlistId, playlistName, total: entries.length, results });
    return res.json({ total: entries.length, results, playlistId, playlistName });
  }

  // ── SINGLE VIDEO ──
  const videoId = getVideoId(url);
  if (!videoId) return res.status(400).json({ error: 'Invalid YouTube URL' });

  if (downloadedIds.has(videoId)) {
    return res.json({ skipped: true, videoId, reason: 'already downloaded' });
  }

  try {
    const r = await downloadAndUpload(url, videoId, null);
    downloadedIds.add(videoId);
    saveIds(downloadedIds);
    res.json({ success: true, ...r });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/music', async (req, res) => {
  try {
    // Local files
    const localFiles = [];
    if (fs.existsSync(DOWNLOAD_DIR)) {
      for (const file of fs.readdirSync(DOWNLOAD_DIR)) {
        if (!file.endsWith('.mp3') || file === 'downloaded_ids.json') continue;
        const stat = fs.statSync(path.join(DOWNLOAD_DIR, file));
        const vid = file.match(/^([a-zA-Z0-9_-]{11})_/)?.[1] || null;
        const plInfo = vid ? playlistInfo[vid] : null;
        localFiles.push({
          name: file,
          path: `/downloads/${file}`,
          size: stat.size,
          downloadedAt: stat.mtime.toISOString(),
          videoId: vid,
          playlistId: plInfo?.playlistId || null,
          playlistName: plInfo?.name || null,
          source: 'local'
        });
      }
    }

    // GitHub files
    let githubFiles = [];
    try {
      const ghFiles = await ghGetAllMp3s();
      githubFiles = ghFiles.map(f => {
        const name = path.basename(f.path);
        const vid = name.match(/^([a-zA-Z0-9_-]{11})_/)?.[1] || null;
        const plInfo = vid ? playlistInfo[vid] : null;
        return {
          name,
          path: `https://raw.githubusercontent.com/${GITHUB_OWNER}/${GITHUB_REPO}/${GITHUB_BRANCH}/${f.path}`,
          size: f.size,
          downloadedAt: null,
          videoId: vid,
          playlistId: plInfo?.playlistId || null,
          playlistName: plInfo?.name || null,
          source: 'github'
        };
      });
    } catch (e) {
      console.warn('GitHub fetch failed:', e.message);
    }

    // Merge: local wins, no duplicates
    const localNames = new Set(localFiles.map(f => f.name));
    const allFiles = [...localFiles, ...githubFiles.filter(f => !localNames.has(f.name))];

    // Build playlist groups
    const singles = [];
    const playlistsMap = new Map();

    for (const f of allFiles) {
      if (f.playlistId) {
        if (!playlistsMap.has(f.playlistId)) {
          playlistsMap.set(f.playlistId, {
            playlistId: f.playlistId,
            name: f.playlistName || f.playlistId,
            url: f.url || null,
            tracks: []
          });
        }
        playlistsMap.get(f.playlistId).tracks.push(f);
      } else {
        singles.push(f);
      }
    }

    const groups = [
      ...([...playlistsMap.values()].map(p => ({
        ...p,
        tracks: p.tracks.sort((a, b) => a.name.localeCompare(b.name))
      }))),
      ...(singles.length ? [{ playlistId: null, name: 'Músicas Avulsas', tracks: singles.sort((a, b) => a.name.localeCompare(b.name)) }] : [])
    ];

    res.json({ total: allFiles.length, groups });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/delete', async (req, res) => {
  const { files } = req.body;
  if (!Array.isArray(files) || files.length === 0) {
    return res.status(400).json({ error: 'files array required' });
  }

  const results = [];
  for (const filename of files) {
    try {
      const existing = await ghGetContents(filename);
      if (!existing) { results.push({ filename, status: 'not_found' }); continue; }
      await ghDeleteFile(filename, existing.sha, `Delete: ${filename}`);
      const localPath = path.join(DOWNLOAD_DIR, filename);
      if (fs.existsSync(localPath)) fs.unlinkSync(localPath);
      const vid = filename.match(/^([a-zA-Z0-9_-]{11})_/)?.[1];
      if (vid && downloadedIds.has(vid)) {
        downloadedIds.delete(vid);
        delete playlistInfo[vid];
      }
      results.push({ filename, status: 'deleted' });
    } catch (e) {
      results.push({ filename, status: 'error', error: e.message });
    }
  }

  saveIds(downloadedIds);
  savePlaylistInfo(playlistInfo);
  sseSend('files-deleted', { files: results });
  res.json({ results });
});

app.post('/api/sync', async (req, res) => {
  try {
    const files = fs.readdirSync(DOWNLOAD_DIR)
      .filter(f => f.endsWith('.mp3') && f !== 'downloaded_ids.json');
    const results = [];
    for (const file of files) {
      const content = fs.readFileSync(path.join(DOWNLOAD_DIR, file));
      const status = await ghUploadFile(content, file, `Add: ${file}`);
      results.push({ file, status });
    }
    res.json({ synced: results.length, results });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.use('/downloads', express.static(DOWNLOAD_DIR));

app.listen(PORT, () => {
  console.log(`\n🎵 Music Downloader → http://localhost:${PORT}`);
  console.log(`📁 ${DOWNLOAD_DIR}`);
  console.log(`🔗 ${GITHUB_OWNER}/${GITHUB_REPO}`);
  console.log(`📊 ${downloadedIds.size} tracks cached\n`);
});
