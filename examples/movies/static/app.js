(() => {
  'use strict';

  let isAdminMode = false;
  let currentMovies = [];
  let currentPlayingIndex = -1;
  let selectedUploadFiles = [];

  // Repeat mode state: 'off' | 'all' | 'one'
  let repeatMode = 'off';

  // DOM Elements
  const modeViewerBtn = document.getElementById('modeViewerBtn');
  const modeAdminBtn = document.getElementById('modeAdminBtn');
  const viewerView = document.getElementById('viewerView');
  const adminView = document.getElementById('adminView');
  const adminTableBody = document.getElementById('adminTableBody');

  const metricTotalSize = document.getElementById('metricTotalSize');
  const metricTotalTitles = document.getElementById('metricTotalTitles');

  const grid = document.getElementById('grid');
  const emptyState = document.getElementById('emptyState');
  const searchInput = document.getElementById('searchInput');
  const searchBtn = document.getElementById('searchBtn');
  const genreChips = document.getElementById('genreChips');
  const categoryBar = document.getElementById('categoryBar');
  const toggleSidebarBtn = document.getElementById('toggleSidebarBtn');
  const sidebar = document.getElementById('sidebar');

  // Multi-Upload Modal Elements
  const uploadModal = document.getElementById('uploadModal');
  const uploadForm = document.getElementById('uploadForm');
  const uploadSubmitBtn = document.getElementById('uploadSubmitBtn');
  const uploadProgress = document.getElementById('uploadProgress');
  const progressFill = document.getElementById('progressFill');
  const batchProgressText = document.getElementById('batchProgressText');
  const batchProgressPercent = document.getElementById('batchProgressPercent');
  const uploadError = document.getElementById('uploadError');

  const dropzone = document.getElementById('dropzone');
  const fieldVideo = document.getElementById('fieldVideo');
  const queueContainer = document.getElementById('queueContainer');
  const fileQueueList = document.getElementById('fileQueueList');
  const fileCount = document.getElementById('fileCount');

  // Player Stage Elements
  const playerModal = document.getElementById('playerModal');
  const playerTitle = document.getElementById('playerTitle');
  const playerVideo = document.getElementById('playerVideo');
  const playerGenre = document.getElementById('playerGenre');
  const playerYear = document.getElementById('playerYear');
  const playerSize = document.getElementById('playerSize');
  const playerDeleteBtn = document.getElementById('playerDeleteBtn');
  const playlistQueue = document.getElementById('playlistQueue');
  const queueCount = document.getElementById('queueCount');
  const centerPlayBtn = document.getElementById('centerPlayBtn');

  // Extended Custom Controls
  const playPauseBtn = document.getElementById('playPauseBtn');
  const prevBtn = document.getElementById('prevBtn');
  const nextBtn = document.getElementById('nextBtn');
  const repeatBtn = document.getElementById('repeatBtn');
  const repeatBadge = document.getElementById('repeatBadge');
  const muteBtn = document.getElementById('muteBtn');
  const volumeSlider = document.getElementById('volumeSlider');
  const currentTimeEl = document.getElementById('currentTime');
  const durationTimeEl = document.getElementById('durationTime');
  const progressBarContainer = document.getElementById('progressBarContainer');
  const progressBar = document.getElementById('progressBar');
  const bufferedBar = document.getElementById('bufferedBar');
  const speedSelect = document.getElementById('speedSelect');
  const pipBtn = document.getElementById('pipBtn');
  const fullscreenBtn = document.getElementById('fullscreenBtn');

  // ── Modal Visibility Helpers ─────────────────────────────────────────────

  function showModal(element) {
    if (!element) return;
    element.classList.remove('hidden');
    element.removeAttribute('hidden');
  }

  function hideModal(element) {
    if (!element) return;
    element.classList.add('hidden');
    element.setAttribute('hidden', '');
  }

  // Ensure modals are strictly hidden on startup
  hideModal(playerModal);
  hideModal(uploadModal);

  // ── Sidebar Toggle ───────────────────────────────────────────────────────

  if (toggleSidebarBtn && sidebar) {
    toggleSidebarBtn.addEventListener('click', () => {
      sidebar.classList.toggle('-ml-56');
    });
  }

  // ── Mode Switcher ────────────────────────────────────────────────────────

  function setMode(admin) {
    isAdminMode = admin;
    if (isAdminMode) {
      modeAdminBtn.className = 'px-3 py-1 rounded-full bg-brand-500 text-white shadow-sm transition-all';
      modeViewerBtn.className = 'px-3 py-1 rounded-full text-zinc-400 hover:text-white transition-all';
      viewerView.classList.add('hidden');
      adminView.classList.remove('hidden');
    } else {
      modeViewerBtn.className = 'px-3 py-1 rounded-full bg-brand-500 text-white shadow-sm transition-all';
      modeAdminBtn.className = 'px-3 py-1 rounded-full text-zinc-400 hover:text-white transition-all';
      adminView.classList.add('hidden');
      viewerView.classList.remove('hidden');
    }

    document.querySelectorAll('.admin-only').forEach(el => {
      if (isAdminMode) el.classList.remove('hidden');
      else el.classList.add('hidden');
    });

    renderAdminDashboard();
  }

  modeViewerBtn.addEventListener('click', () => setMode(false));
  modeAdminBtn.addEventListener('click', () => setMode(true));

  // ── Filename Heuristic Inference ─────────────────────────────────────────

  function parseFilename(filename) {
    let name = filename.replace(/\.[^/.]+$/, '');
    let year = '';

    const yearMatch = name.match(/\b(19\d\d|20[0-2]\d)\b/);
    if (yearMatch) {
      year = yearMatch[1];
      name = name.substring(0, yearMatch.index);
    }

    const tagsRegex = /\b(480p|720p|1080p|2160p|4k|x264|x265|hevc|h264|web-dl|webrip|bluray|brrip|hdrip|dvdrip|aac|dd5\.1|remux|hdr)\b/gi;
    name = name.replace(tagsRegex, '');
    name = name.replace(/[\._\-\[\]]/g, ' ').replace(/\s+/g, ' ').trim();

    const title = name.split(' ')
      .map(w => w ? w.charAt(0).toUpperCase() + w.slice(1).toLowerCase() : '')
      .join(' ');

    return { title: title || filename, year: year || '' };
  }

  function formatBytes(bytes) {
    if (!bytes || bytes <= 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
    return (bytes / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1) + ' ' + units[i];
  }

  function formatTime(seconds) {
    if (isNaN(seconds)) return '00:00';
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
  }

  function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str == null ? '' : String(str);
    return div.innerHTML;
  }

  function initials(title) {
    const words = title.trim().split(/\s+/).slice(0, 2);
    return words.map(w => w[0] ? w[0].toUpperCase() : '').join('') || '?';
  }

  // ── Catalog & View Renderer ─────────────────────────────────────────────

  async function fetchCatalog(genreFilter = '') {
    const params = new URLSearchParams();
    const q = searchInput ? searchInput.value.trim() : '';
    if (q) params.set('q', q);
    if (genreFilter) params.set('genre', genreFilter);

    try {
      const resp = await fetch('/api/movies?' + params.toString());
      if (!resp.ok) return;
      const data = await resp.json();
      currentMovies = data.movies || [];

      renderCategoryPills(data.genres || [], genreFilter);
      renderGrid(currentMovies);
      renderAdminDashboard();
    } catch (e) {
      console.error('Failed to fetch catalog:', e);
    }
  }

  function renderCategoryPills(genres, activeGenre) {
    if (!categoryBar) return;
    const pillHtml = `
      <button class="genre-pill px-3.5 py-1.5 rounded-lg text-xs font-semibold shrink-0 transition-all ${!activeGenre ? 'bg-white text-black' : 'bg-zinc-900 text-zinc-400 hover:text-white'}" data-genre="">All</button>
      ${genres.map(g => `
        <button class="genre-pill px-3.5 py-1.5 rounded-lg text-xs font-semibold shrink-0 transition-all ${activeGenre === g ? 'bg-white text-black' : 'bg-zinc-900 text-zinc-400 hover:text-white'}" data-genre="${escapeHtml(g)}">${escapeHtml(g)}</button>
      `).join('')}
    `;

    categoryBar.innerHTML = pillHtml;

    if (genreChips) {
      genreChips.innerHTML = genres.map(g => `
        <a href="#" class="sidebar-genre-link flex items-center justify-between px-3 py-1.5 rounded-lg text-zinc-400 hover:bg-zinc-900 hover:text-white transition-colors ${activeGenre === g ? 'bg-zinc-900 text-white font-semibold' : ''}" data-genre="${escapeHtml(g)}">
          <span>${escapeHtml(g)}</span>
        </a>
      `).join('');

      genreChips.querySelectorAll('.sidebar-genre-link').forEach(link => {
        link.addEventListener('click', (e) => {
          e.preventDefault();
          fetchCatalog(link.getAttribute('data-genre'));
        });
      });
    }

    categoryBar.querySelectorAll('.genre-pill').forEach(btn => {
      btn.addEventListener('click', () => fetchCatalog(btn.getAttribute('data-genre')));
    });
  }

  function renderGrid(movies) {
    if (!grid) return;
    if (movies.length === 0) {
      grid.innerHTML = '';
      showModal(emptyState);
      return;
    }
    hideModal(emptyState);

    grid.innerHTML = movies.map(m => {
      const posterCell = m.hasPoster
        ? `<img class="w-full h-full object-cover transition-transform duration-500 group-hover:scale-105" src="/api/movies/${encodeURIComponent(m.key)}/poster" alt="${escapeHtml(m.title)}">`
        : `<div class="w-full h-full flex items-center justify-center bg-zinc-900 text-zinc-600 font-black text-2xl group-hover:text-brand-500 transition-colors">${escapeHtml(initials(m.title))}</div>`;

      return `
        <div class="card group relative bg-zinc-900/40 border border-zinc-800/80 rounded-2xl overflow-hidden cursor-pointer hover:border-brand-500/50 hover:shadow-2xl hover:shadow-brand-500/10 transition-all duration-300 flex flex-col" data-key="${escapeHtml(m.key)}">
          <div class="relative aspect-[2/3] w-full overflow-hidden bg-zinc-950">
            ${posterCell}
            <div class="absolute inset-0 bg-black/40 opacity-0 group-hover:opacity-100 transition-opacity duration-300 flex items-center justify-center backdrop-blur-[2px]">
              <div class="w-12 h-12 rounded-full bg-brand-500 text-white flex items-center justify-center shadow-lg transform scale-75 group-hover:scale-100 transition-transform duration-300">
                <svg class="w-6 h-6 fill-current translate-x-0.5" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg>
              </div>
            </div>
            ${m.year ? `<span class="absolute bottom-2 right-2 bg-black/80 backdrop-blur-md px-2 py-0.5 rounded text-[10px] font-mono text-zinc-300">${escapeHtml(m.year)}</span>` : ''}
          </div>
          <div class="p-4 flex flex-col justify-between flex-1">
            <div>
              <h3 class="font-bold text-xs text-zinc-100 group-hover:text-brand-400 transition-colors line-clamp-1">${escapeHtml(m.title)}</h3>
              <p class="text-[11px] text-zinc-500 mt-1 font-medium">${escapeHtml(m.genre || 'General')} • ${formatBytes(m.size)}</p>
            </div>
          </div>
        </div>
      `;
    }).join('');

    grid.querySelectorAll('.card').forEach(card => {
      card.addEventListener('click', () => {
        const key = card.getAttribute('data-key');
        const index = currentMovies.findIndex(m => m.key === key);
        if (index !== -1) playMovieAtIndex(index);
      });
    });
  }

  function renderAdminDashboard() {
    if (!isAdminMode) return;

    const totalBytes = currentMovies.reduce((acc, m) => acc + (m.size || 0), 0);
    if (metricTotalSize) metricTotalSize.textContent = formatBytes(totalBytes);
    if (metricTotalTitles) metricTotalTitles.textContent = currentMovies.length;

    if (!adminTableBody) return;
    adminTableBody.innerHTML = currentMovies.map(m => `
      <tr class="hover:bg-zinc-800/40 transition-colors">
        <td class="p-4 font-semibold text-white flex items-center gap-3">
          <div class="w-8 h-8 bg-zinc-800 rounded-lg overflow-hidden flex items-center justify-center text-xs font-bold text-zinc-500 shrink-0">
            ${m.hasPoster ? `<img src="/api/movies/${encodeURIComponent(m.key)}/poster" class="w-full h-full object-cover">` : initials(m.title)}
          </div>
          <span>${escapeHtml(m.title)}</span>
        </td>
        <td class="p-4 text-zinc-400">${escapeHtml(m.genre || '—')}</td>
        <td class="p-4 text-zinc-400 font-mono">${escapeHtml(m.year || '—')}</td>
        <td class="p-4 text-zinc-400 font-mono">${formatBytes(m.size)}</td>
        <td class="p-4 text-right">
          <button class="delete-movie-btn bg-red-500/10 hover:bg-red-600 text-red-400 hover:text-white px-3 py-1 rounded-lg text-xs font-semibold border border-red-500/20 transition-all cursor-pointer" data-key="${escapeHtml(m.key)}" data-title="${escapeHtml(m.title)}">
            Delete
          </button>
        </td>
      </tr>
    `).join('');

    adminTableBody.querySelectorAll('.delete-movie-btn').forEach(btn => {
      btn.addEventListener('click', () => deleteMovie(btn.getAttribute('data-key'), btn.getAttribute('data-title')));
    });
  }

  async function deleteMovie(key, title) {
    if (!confirm(`Delete "${title}"?`)) return;
    try {
      const resp = await fetch('/api/movies/' + encodeURIComponent(key), { method: 'DELETE' });
      if (!resp.ok) throw new Error('Delete request failed');
      if (!playerModal.classList.contains('hidden')) closePlayer();
      await fetchCatalog();
    } catch (e) {
      alert('Delete failed: ' + e.message);
    }
  }

  if (playerDeleteBtn) {
    playerDeleteBtn.addEventListener('click', () => {
      if (currentPlayingIndex >= 0 && currentPlayingIndex < currentMovies.length) {
        const movie = currentMovies[currentPlayingIndex];
        deleteMovie(movie.key, movie.title);
      }
    });
  }

  // ── Drag & Drop Multi-Upload Pipeline ──────────────────────────────────

  if (dropzone && fieldVideo) {
    dropzone.addEventListener('click', () => fieldVideo.click());

    ['dragenter', 'dragover'].forEach(eventName => {
      dropzone.addEventListener(eventName, (e) => { e.preventDefault(); dropzone.classList.add('border-brand-500', 'bg-brand-500/5'); });
    });

    ['dragleave', 'drop'].forEach(eventName => {
      dropzone.addEventListener(eventName, (e) => { e.preventDefault(); dropzone.classList.remove('border-brand-500', 'bg-brand-500/5'); });
    });

    dropzone.addEventListener('drop', (e) => {
      const files = Array.from(e.dataTransfer.files).filter(f => f.type.startsWith('video/'));
      handleFileSelection(files);
    });

    fieldVideo.addEventListener('change', (e) => {
      handleFileSelection(Array.from(e.target.files));
    });
  }

  function handleFileSelection(files) {
    if (!files || files.length === 0) return;

    selectedUploadFiles = files.map((file, id) => {
      const inferred = parseFilename(file.name);
      return { id, file, title: inferred.title, year: inferred.year, genre: '' };
    });

    if (fileCount) fileCount.textContent = selectedUploadFiles.length;
    renderUploadQueue();
    if (queueContainer) queueContainer.classList.remove('hidden');
  }

  function renderUploadQueue() {
    if (!fileQueueList) return;
    fileQueueList.innerHTML = selectedUploadFiles.map((item, idx) => `
      <div class="bg-zinc-950 border border-zinc-800 p-3.5 rounded-xl space-y-3" data-id="${item.id}">
        <div class="flex items-center justify-between gap-2 text-xs">
          <span class="font-mono text-zinc-400 truncate max-w-sm">${escapeHtml(item.file.name)}</span>
          <div class="flex items-center gap-3">
            <span class="text-zinc-500 font-mono">${formatBytes(item.file.size)}</span>
            <button type="button" class="remove-file-btn text-zinc-500 hover:text-red-400 transition-colors" data-idx="${idx}">&times;</button>
          </div>
        </div>

        <div class="grid grid-cols-1 sm:grid-cols-3 gap-2">
          <input type="text" class="queue-title-input px-3 py-1.5 text-xs bg-zinc-900 border border-zinc-800 rounded-lg text-white focus:outline-none focus:border-brand-500" placeholder="Title" value="${escapeHtml(item.title)}">
          <input type="text" class="queue-year-input px-3 py-1.5 text-xs bg-zinc-900 border border-zinc-800 rounded-lg text-white focus:outline-none focus:border-brand-500" placeholder="Year" value="${escapeHtml(item.year)}">
          <input type="text" class="queue-genre-input px-3 py-1.5 text-xs bg-zinc-900 border border-zinc-800 rounded-lg text-white focus:outline-none focus:border-brand-500" placeholder="Genre" value="${escapeHtml(item.genre)}">
        </div>
      </div>
    `).join('');

    fileQueueList.querySelectorAll('.remove-file-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        const idx = parseInt(btn.getAttribute('data-idx'), 10);
        selectedUploadFiles.splice(idx, 1);
        if (fileCount) fileCount.textContent = selectedUploadFiles.length;
        if (selectedUploadFiles.length === 0 && queueContainer) queueContainer.classList.add('hidden');
        else renderUploadQueue();
      });
    });

    fileQueueList.querySelectorAll('[data-id]').forEach((row, idx) => {
      row.querySelector('.queue-title-input').addEventListener('input', (e) => selectedUploadFiles[idx].title = e.target.value);
      row.querySelector('.queue-year-input').addEventListener('input', (e) => selectedUploadFiles[idx].year = e.target.value);
      row.querySelector('.queue-genre-input').addEventListener('input', (e) => selectedUploadFiles[idx].genre = e.target.value);
    });
  }

  // Chunked Upload Execution
  const CHUNK_SIZE = 8 * 1024 * 1024;

  async function sha256Hex(buffer) {
    const digest = await crypto.subtle.digest('SHA-256', buffer);
    return Array.from(new Uint8Array(digest)).map(b => b.toString(16).padStart(2, '0')).join('');
  }

  async function uploadSingleFile(fileItem, onProgress) {
    const beginResp = await fetch('/api/upload/begin', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title: fileItem.title,
        genre: fileItem.genre,
        year: fileItem.year,
        filename: fileItem.file.name,
        size: fileItem.file.size,
        contentType: fileItem.file.type || 'application/octet-stream',
        blockSize: CHUNK_SIZE,
      }),
    });
    if (!beginResp.ok) throw new Error('Upload init failed');
    const session = await beginResp.json();

    for (let offset = 0; offset < fileItem.file.size; offset += CHUNK_SIZE) {
      const end = Math.min(offset + CHUNK_SIZE, fileItem.file.size);
      const blob = fileItem.file.slice(offset, end);
      const buffer = await blob.arrayBuffer();
      const sha = await sha256Hex(buffer);

      const chunkResp = await fetch('/api/upload/chunk', {
        method: 'POST',
        headers: {
          'X-Session-ID': session.sessionId,
          'X-Offset': String(offset),
          'X-Chunk-SHA256': sha,
          'Content-Type': 'application/octet-stream',
        },
        body: buffer,
      });
      if (!chunkResp.ok) throw new Error('Chunk upload failed');
      onProgress(end / fileItem.file.size);
    }

    const completeResp = await fetch('/api/upload/complete', {
      method: 'POST',
      headers: { 'X-Session-ID': session.sessionId },
    });
    if (!completeResp.ok) throw new Error('Finalize failed');
    return completeResp.json();
  }

  if (uploadForm) {
    uploadForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      if (!selectedUploadFiles.length) return;

      uploadSubmitBtn.disabled = true;
      showModal(uploadProgress);

      try {
        for (let i = 0; i < selectedUploadFiles.length; i++) {
          const item = selectedUploadFiles[i];
          if (batchProgressText) batchProgressText.textContent = `File ${i + 1}/${selectedUploadFiles.length}: "${item.title}"`;

          await uploadSingleFile(item, (fileProgress) => {
            const overall = ((i + fileProgress) / selectedUploadFiles.length) * 100;
            if (progressFill) progressFill.style.width = overall + '%';
            if (batchProgressPercent) batchProgressPercent.textContent = Math.round(overall) + '%';
          });
        }
        setTimeout(() => { closeUpload(); fetchCatalog(); }, 500);
      } catch (err) {
        showModal(uploadError);
        if (uploadError) uploadError.textContent = err.message;
      } finally {
        uploadSubmitBtn.disabled = false;
      }
    });
  }

  function openUpload() {
    if (uploadForm) uploadForm.reset();
    hideModal(uploadError);
    hideModal(uploadProgress);
    if (queueContainer) queueContainer.classList.add('hidden');
    selectedUploadFiles = [];
    showModal(uploadModal);
  }

  function closeUpload() {
    hideModal(uploadModal);
  }

  // ── Cinema Player Stage & Navigation Controls ─────────────────────────────

  function playMovieAtIndex(index) {
    if (index < 0 || index >= currentMovies.length) return;
    currentPlayingIndex = index;
    const movie = currentMovies[index];

    if (playerTitle) playerTitle.textContent = movie.title;
    if (playerVideo) playerVideo.src = '/api/movies/' + encodeURIComponent(movie.key) + '/stream';
    if (playerGenre) playerGenre.textContent = movie.genre || 'Video';
    if (playerYear) playerYear.textContent = movie.year || '';
    if (playerSize) playerSize.textContent = formatBytes(movie.size);

    renderPlaylistQueue();
    showModal(playerModal);
    if (playerVideo) playerVideo.play().catch(() => {});
    updatePlayPauseUI();
  }

  function playNextMovie() {
    if (currentMovies.length === 0) return;
    let nextIndex = currentPlayingIndex + 1;
    if (nextIndex >= currentMovies.length) {
      if (repeatMode === 'all') {
        nextIndex = 0;
      } else {
        return;
      }
    }
    playMovieAtIndex(nextIndex);
  }

  function playPrevMovie() {
    if (currentMovies.length === 0) return;
    if (playerVideo && playerVideo.currentTime > 3) {
      playerVideo.currentTime = 0;
      return;
    }
    let prevIndex = currentPlayingIndex - 1;
    if (prevIndex < 0) {
      prevIndex = repeatMode === 'all' ? currentMovies.length - 1 : 0;
    }
    playMovieAtIndex(prevIndex);
  }

  function toggleRepeatMode() {
    if (repeatMode === 'off') {
      repeatMode = 'all';
      repeatBtn.className = 'text-brand-500 transition-colors cursor-pointer relative p-1 rounded-md bg-brand-500/10';
      repeatBtn.title = 'Repeat Mode: All Tracks';
      hideModal(repeatBadge);
    } else if (repeatMode === 'all') {
      repeatMode = 'one';
      repeatBtn.className = 'text-brand-500 transition-colors cursor-pointer relative p-1 rounded-md bg-brand-500/10';
      repeatBtn.title = 'Repeat Mode: Current Track';
      showModal(repeatBadge);
    } else {
      repeatMode = 'off';
      repeatBtn.className = 'text-zinc-500 hover:text-white transition-colors cursor-pointer relative p-1 rounded-md';
      repeatBtn.title = 'Repeat Mode: Off';
      hideModal(repeatBadge);
    }
  }

  function renderPlaylistQueue() {
    if (!queueCount || !playlistQueue) return;
    queueCount.textContent = `(${currentMovies.length})`;
    playlistQueue.innerHTML = currentMovies.map((m, idx) => `
      <div class="queue-item flex items-center gap-3 p-2 rounded-xl cursor-pointer transition-all ${idx === currentPlayingIndex ? 'bg-brand-500/10 border border-brand-500/30' : 'hover:bg-zinc-900'}" data-index="${idx}">
        <div class="w-12 h-8 rounded-lg overflow-hidden bg-zinc-900 shrink-0 flex items-center justify-center text-xs font-bold text-zinc-500">
          ${m.hasPoster ? `<img src="/api/movies/${encodeURIComponent(m.key)}/poster" class="w-full h-full object-cover">` : initials(m.title)}
        </div>
        <div class="flex-1 min-w-0">
          <h4 class="text-xs font-semibold text-zinc-200 truncate ${idx === currentPlayingIndex ? 'text-brand-500' : ''}">${escapeHtml(m.title)}</h4>
          <p class="text-[10px] text-zinc-500 mt-0.5">${formatBytes(m.size)}</p>
        </div>
      </div>
    `).join('');

    playlistQueue.querySelectorAll('.queue-item').forEach(item => {
      item.addEventListener('click', () => playMovieAtIndex(parseInt(item.getAttribute('data-index'), 10)));
    });
  }

  function togglePlay() {
    if (!playerVideo) return;
    if (playerVideo.paused) playerVideo.play();
    else playerVideo.pause();
    updatePlayPauseUI();
  }

  function updatePlayPauseUI() {
    if (!playerVideo) return;
    const isPaused = playerVideo.paused;

    if (playPauseBtn) {
      playPauseBtn.innerHTML = isPaused
        ? `<svg class="w-6 h-6 fill-current" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg>`
        : `<svg class="w-6 h-6 fill-current" viewBox="0 0 24 24"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>`;
    }

    if (centerPlayBtn) {
      if (isPaused) {
        centerPlayBtn.classList.remove('opacity-0', 'scale-75', 'pointer-events-none');
        centerPlayBtn.classList.add('opacity-100', 'scale-100');
      } else {
        centerPlayBtn.classList.remove('opacity-100', 'scale-100');
        centerPlayBtn.classList.add('opacity-0', 'scale-75', 'pointer-events-none');
      }
    }
  }

  if (playerVideo) {
    playerVideo.addEventListener('timeupdate', () => {
      const current = playerVideo.currentTime;
      const duration = playerVideo.duration || 0;
      if (currentTimeEl) currentTimeEl.textContent = formatTime(current);
      if (durationTimeEl) durationTimeEl.textContent = formatTime(duration);

      if (duration > 0 && progressBar) {
        progressBar.style.width = `${(current / duration) * 100}%`;
      }

      if (playerVideo.buffered.length > 0 && duration > 0 && bufferedBar) {
        const bufferedEnd = playerVideo.buffered.end(playerVideo.buffered.length - 1);
        bufferedBar.style.width = `${(bufferedEnd / duration) * 100}%`;
      }
    });

    playerVideo.addEventListener('play', updatePlayPauseUI);
    playerVideo.addEventListener('pause', updatePlayPauseUI);

    playerVideo.addEventListener('ended', () => {
      if (repeatMode === 'one') {
        playerVideo.currentTime = 0;
        playerVideo.play();
      } else {
        playNextMovie();
      }
    });

    playerVideo.addEventListener('click', togglePlay);
  }

  if (centerPlayBtn) centerPlayBtn.addEventListener('click', togglePlay);
  if (playPauseBtn) playPauseBtn.addEventListener('click', togglePlay);
  if (prevBtn) prevBtn.addEventListener('click', playPrevMovie);
  if (nextBtn) nextBtn.addEventListener('click', playNextMovie);
  if (repeatBtn) repeatBtn.addEventListener('click', toggleRepeatMode);

  if (progressBarContainer && playerVideo) {
    progressBarContainer.addEventListener('click', (e) => {
      const rect = progressBarContainer.getBoundingClientRect();
      const pos = (e.clientX - rect.left) / rect.width;
      if (playerVideo.duration) playerVideo.currentTime = pos * playerVideo.duration;
    });
  }

  if (muteBtn && playerVideo) {
    muteBtn.addEventListener('click', () => {
      playerVideo.muted = !playerVideo.muted;
      if (volumeSlider) volumeSlider.value = playerVideo.muted ? 0 : playerVideo.volume;
    });
  }

  if (volumeSlider && playerVideo) {
    volumeSlider.addEventListener('input', (e) => {
      playerVideo.volume = e.target.value;
      playerVideo.muted = e.target.value === '0';
    });
  }

  if (speedSelect && playerVideo) {
    speedSelect.addEventListener('change', (e) => {
      playerVideo.playbackRate = parseFloat(e.target.value);
    });
  }

  if (pipBtn && playerVideo) {
    pipBtn.addEventListener('click', () => {
      if (document.pictureInPictureElement) document.exitPictureInPicture();
      else if (document.pictureInPictureEnabled) playerVideo.requestPictureInPicture();
    });
  }

  if (fullscreenBtn && playerModal) {
    fullscreenBtn.addEventListener('click', () => {
      if (!document.fullscreenElement) playerModal.requestFullscreen();
      else document.exitFullscreen();
    });
  }

  function closePlayer() {
    hideModal(playerModal);
    if (playerVideo) {
      playerVideo.pause();
      playerVideo.removeAttribute('src');
      playerVideo.load();
    }
    currentPlayingIndex = -1;
  }

  // ── Global Event Delegation & Shortcuts ──────────────────────────────────

  document.addEventListener('click', (e) => {
    const actionEl = e.target.closest('[data-action]');
    if (!actionEl) return;

    const action = actionEl.getAttribute('data-action');
    if (action === 'open-upload') openUpload();
    if (action === 'close-upload') closeUpload();
    if (action === 'close-player') closePlayer();
  });

  // Global Keyboard Shortcuts
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      if (!playerModal.classList.contains('hidden')) {
        closePlayer();
        return;
      }
      if (!uploadModal.classList.contains('hidden')) {
        closeUpload();
        return;
      }
    }

    if (!playerModal.classList.contains('hidden') && playerVideo) {
      if (e.code === 'Space') {
        e.preventDefault();
        togglePlay();
      } else if (e.code === 'ArrowLeft') {
        e.preventDefault();
        playerVideo.currentTime = Math.max(0, playerVideo.currentTime - 5);
      } else if (e.code === 'ArrowRight') {
        e.preventDefault();
        playerVideo.currentTime = Math.min(playerVideo.duration || 0, playerVideo.currentTime + 5);
      } else if (e.key.toLowerCase() === 'n') {
        e.preventDefault();
        playNextMovie();
      } else if (e.key.toLowerCase() === 'p') {
        e.preventDefault();
        playPrevMovie();
      } else if (e.key.toLowerCase() === 'r') {
        e.preventDefault();
        toggleRepeatMode();
      } else if (e.key.toLowerCase() === 'f') {
        e.preventDefault();
        if (fullscreenBtn) fullscreenBtn.click();
      } else if (e.key.toLowerCase() === 'm') {
        e.preventDefault();
        if (muteBtn) muteBtn.click();
      }
    }
  });

  // Search input handling
  let searchDebounce;
  if (searchInput) {
    searchInput.addEventListener('input', () => {
      clearTimeout(searchDebounce);
      searchDebounce = setTimeout(() => fetchCatalog(), 250);
    });
    searchInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') fetchCatalog();
    });
  }

  if (searchBtn) {
    searchBtn.addEventListener('click', () => fetchCatalog());
  }

  // Initial Boot
  setMode(false);
  fetchCatalog();
})();
