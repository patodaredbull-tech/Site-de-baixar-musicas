'use strict';
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
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

// ─── Deduplication ─────────────────────────────────────────────────────────

const DOWNLOADED_IDS_FILE = path.join(__dirname, 'downloaded_ids.json');

function loadDownloadedIds() {
  try {
    if (fs.existsSync(DOWNLOADED_IDS_FILE)) {
      return new Set(JSON.parse(fs.readFileSync(DOWNLOADED_IDS_FILE, 'utf8')));
    }
  } catch {}
  return new Set();
}

function saveDownloadedIds(ids) {
  fs.writeFileSync(DOWNLOADED_IDS_FILE, JSON.stringify([...ids], null, 2));
}

let downloadedIds = loadDownloadedIds();

function getVideoId(url) {
  const m = url.match(/(?:v=|\/shorts\/|youtu\.be\/)([a-zA-Z0-9_-]{11})/);
  return m ? m[1] : null;
}

function isPlaylistUrl(url) {
  // music.youtube.com playlists have ?list= but no ?v=
  return /[?&]list=/.test(url) && !/[?&]v=/.test(url) && !/\/shorts\//.test(url);
}

function ytArgs(url) {
  // yt-dlp args: try harder for music.youtube.com and age-gated videos
  const base = [
    '--newline', '--no-playlist', '--no-update',
    '-x', '--audio-format', 'mp3', '--audio-quality', '0',
    '--embed-metadata', '--no-progress',
    '--extractor-args', 'youtube:player_client=web,default'
  ];
  // yt-dlp handles music.youtube.com natively, no extra args needed
  return base;
}

// ─── SSE clients ───────────────────────────────────────────────────────────

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
    payload,
    { headers: GH_HEADERS }
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
    if (tree.truncated !== false && tree.tree.length < 100) break;
    if (tree.tree.length < 100) break;
    page++;
  }
  return files;
}

// ─── yt-dlp download ───────────────────────────────────────────────────────

async function downloadSingle(url, videoId) {
  return new Promise((resolve, reject) => {
    const outputTemplate = path.join(DOWNLOAD_DIR, '%(id)s_%(title)s.%(ext)s');
    const args = [...ytArgs(url), '-o', outputTemplate, url];
    const proc = spawn('yt-dlp', args, { shell: true });

    let stderr = '';
    proc.stderr.on('data', d => { stderr += d.toString(); });
    proc.on('close', code => {
      if (code !== 0) return reject(new Error(stderr || `yt-dlp exited ${code}`));
      const files = fs.readdirSync(DOWNLOAD_DIR)
        .filter(f => f.startsWith(videoId) && f.endsWith('.mp3'));
      if (files.length > 0) resolve(files[0]);
      else reject(new Error('Download completed but file not found'));
    });
    proc.on('error', reject);
  });
}

async function downloadOne(url, videoId) {
  sseSend('download-start', { videoId, url });

  try {
    const filename = await downloadSingle(url, videoId);
    const filePath = path.join(DOWNLOAD_DIR, filename);
    const content = fs.readFileSync(filePath);

    const status = await ghUploadFile(content, filename, `Add: ${filename}`);
    sseSend('download-done', { videoId, filename, status });
    return { success: true, filename, status };
  } catch (e) {
    sseSend('download-error', { videoId, error: e.message });
    throw e;
  }
}

// ─── Routes ────────────────────────────────────────────────────────────────

// GET /api/status — server health check
app.get('/api/status', (req, res) => {
  res.json({ ok: true, downloadedCount: downloadedIds.size });
});

// GET /api/sse — SSE stream for live updates
app.get('/api/sse', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  // Send initial state
  res.write(`event: connected\ndata: ${JSON.stringify({ downloadedCount: downloadedIds.size })}\n\n`);

  sseClients.add(res);

  req.on('close', () => {
    sseClients.delete(res);
    res.end();
  });
});

// POST /api/download — queue download (single or playlist)
app.post('/api/download', async (req, res) => {
  const { url } = req.body;
  if (!url) return res.status(400).json({ error: 'URL required' });

  // Playlist → get all entries first
  if (isPlaylistUrl(url)) {
    try {
      sseSend('playlist-start', { url });
      const infoData = await new Promise((resolve, reject) => {
        const p = spawn('yt-dlp', [
          '--flat-playlist', '-J', '--no-warnings',
          '--extractor-args', 'youtube:player_client=web,default',
          url
        ], { shell: true });
        let d = '';
        p.stdout.on('data', x => d += x.toString());
        p.stderr.on('data', () => { /* suppress yt-dlp warnings */ });
        p.on('close', code => {
          // yt-dlp exits 1 on playlist fetch even on success (due to warnings) — parse stdout if we got data
          if (d.trim()) resolve(d);
          else reject(new Error(d || `yt-dlp exited ${code}`));
        });
        p.on('error', reject);
      });
      const playlist = JSON.parse(infoData);
      const entries = (playlist.entries || []).filter(Boolean);
      const results = [];

      for (const entry of entries) {
        const vid = entry.id;
        if (downloadedIds.has(vid)) {
          results.push({ videoId: vid, title: entry.title, status: 'skipped' });
          sseSend('download-skipped', { videoId: vid, title: entry.title });
          continue;
        }
        try {
          const r = await downloadOne(`https://www.youtube.com/watch?v=${vid}`, vid);
          downloadedIds.add(vid);
          saveDownloadedIds(downloadedIds);
          results.push({ videoId: vid, title: entry.title, status: 'done', filename: r.filename });
        } catch (e) {
          results.push({ videoId: vid, title: entry.title, status: 'error', error: e.message });
          sseSend('download-error', { videoId: vid, error: e.message });
        }
      }

      sseSend('playlist-done', { total: entries.length, results });
      return res.json({ total: entries.length, results });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // Single video
  const videoId = getVideoId(url);
  if (!videoId) return res.status(400).json({ error: 'Invalid YouTube URL' });

  if (downloadedIds.has(videoId)) {
    return res.json({ skipped: true, videoId, reason: 'already downloaded' });
  }

  try {
    const r = await downloadOne(url, videoId);
    downloadedIds.add(videoId);
    saveDownloadedIds(downloadedIds);
    res.json({ success: true, ...r });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/music — list all MP3s (local + GitHub merged)
app.get('/api/music', async (req, res) => {
  try {
    // Local files
    const localFiles = [];
    if (fs.existsSync(DOWNLOAD_DIR)) {
      for (const file of fs.readdirSync(DOWNLOAD_DIR)) {
        if (!file.endsWith('.mp3') || file === 'downloaded_ids.json') continue;
        const stat = fs.statSync(path.join(DOWNLOAD_DIR, file));
        const vid = file.match(/^([a-zA-Z0-9_-]{11})_/)?.[1] || null;
        localFiles.push({
          name: file,
          path: `/downloads/${file}`,
          size: stat.size,
          downloadedAt: stat.mtime.toISOString(),
          videoId: vid,
          source: 'local'
        });
      }
    }

    // GitHub files
    let githubFiles = [];
    try {
      const ghFiles = await ghGetAllMp3s();
      githubFiles = ghFiles.map(f => ({
        name: path.basename(f.path),
        path: `https://raw.githubusercontent.com/${GITHUB_OWNER}/${GITHUB_REPO}/${GITHUB_BRANCH}/${f.path}`,
        size: f.size,
        downloadedAt: null,
        videoId: f.path.match(/^([a-zA-Z0-9_-]{11})_/)?.[1] || null,
        source: 'github'
      }));
    } catch (e) {
      console.warn('GitHub fetch failed:', e.message);
    }

    // Merge: local wins over github, no duplicates
    const localNames = new Set(localFiles.map(f => f.name));
    const githubOnly = githubFiles.filter(f => !localNames.has(f.name));
    const allFiles = [...localFiles, ...githubOnly];
    allFiles.sort((a, b) => a.name.localeCompare(b.name));

    res.json({ local: localFiles, all: allFiles, total: allFiles.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/delete — delete from GitHub (and optionally locally)
app.post('/api/delete', async (req, res) => {
  const { files } = req.body;
  if (!Array.isArray(files) || files.length === 0) {
    return res.status(400).json({ error: 'files array required' });
  }

  const results = [];
  for (const filename of files) {
    try {
      const existing = await ghGetContents(filename);
      if (!existing) {
        results.push({ filename, status: 'not_found' });
        continue;
      }
      await ghDeleteFile(filename, existing.sha, `Delete: ${filename}`);
      // Remove from local downloads too
      const localPath = path.join(DOWNLOAD_DIR, filename);
      if (fs.existsSync(localPath)) fs.unlinkSync(localPath);
      // Remove from downloadedIds if we have the videoId
      const vid = filename.match(/^([a-zA-Z0-9_-]{11})_/)?.[1];
      if (vid && downloadedIds.has(vid)) {
        downloadedIds.delete(vid);
        saveDownloadedIds(downloadedIds);
      }
      results.push({ filename, status: 'deleted' });
    } catch (e) {
      results.push({ filename, status: 'error', error: e.message });
    }
  }

  sseSend('files-deleted', { files: results });
  res.json({ results });
});

// Serve downloads
app.use('/downloads', express.static(DOWNLOAD_DIR));

// POST /api/sync — sync all local files to GitHub
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

app.listen(PORT, () => {
  console.log(`\n🎵 Music Downloader → http://localhost:${PORT}`);
  console.log(`📁 ${DOWNLOAD_DIR}`);
  console.log(`🔗 ${GITHUB_OWNER}/${GITHUB_REPO}`);
  console.log(`📊 ${downloadedIds.size} tracks cached\n`);
});
