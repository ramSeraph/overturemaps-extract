// Overture Maps Extract — Main application
// MapLibre GL map with Carto Dark basemap + download panel

import {
  GeoParquetExtractor,
  ExtentData,
  HyparquetBboxReader,
  initDuckDB,
  formatSize,
  getStorageEstimate,
} from 'geoparquet-extractor';

import { resolveStacCatalog } from './stac_resolver.js';
import { OvertureSourceResolver } from './overture_metadata_provider.js';
import { registerCorrectionProtocol } from '@india-boundary-corrector/maplibre-protocol';

const DUCKDB_DIST = 'https://cdn.jsdelivr.net/npm/duckdb-wasm-opfs-tempdir@1.33.0/dist';

// Derive gpkg worker URL from the library's import map entry.
const _libUrl = import.meta.resolve('geoparquet-extractor');
const _libVersion = _libUrl.match(/@([\d.]+)/)?.[1];
const GPKG_WORKER_URL = _libVersion
  ? `https://cdn.jsdelivr.net/npm/geoparquet-extractor@${_libVersion}/dist/gpkg_worker.js`
  : new URL('gpkg_worker.js', _libUrl).href;

// Memory config: 50% of device RAM, clamped to [512MB, maxMB], step 128MB
const MEMORY_STEP = 128;
const MEMORY_MIN_MB = 512;

function getDeviceMaxMemoryMB() {
  const deviceMemGB = navigator.deviceMemory || 4;
  return Math.max(MEMORY_MIN_MB, Math.floor(deviceMemGB * 1024 * 0.75 / MEMORY_STEP) * MEMORY_STEP);
}

function getDefaultMemoryLimitMB() {
  const deviceMemGB = navigator.deviceMemory || 4;
  const halfMB = Math.floor(deviceMemGB * 1024 * 0.5 / MEMORY_STEP) * MEMORY_STEP;
  return Math.max(MEMORY_MIN_MB, Math.min(halfMB, getDeviceMaxMemoryMB()));
}

function formatMemory(mb) {
  return mb >= 1024 ? `${(mb / 1024).toFixed(1)} GB` : `${mb} MB`;
}

const FORMAT_OPTIONS = {
  geojson:     { label: 'GeoJSON',            ext: '.geojson' },
  geojsonseq:  { label: 'GeoJSONSeq',         ext: '.geojsonl' },
  geoparquet:  { label: 'GeoParquet v1.1',    ext: '.parquet' },
  geoparquet2: { label: 'GeoParquet v2.0',    ext: '.parquet' },
  geopackage:  { label: 'GeoPackage',         ext: '.gpkg' },
  csv:         { label: 'CSV with WKT geometry', ext: '.csv' },
  shapefile:   { label: 'Shapefile',           ext: '.shp' },
  kml:         { label: 'KML',                 ext: '.kml' },
};

// --- DOM references ---

const layerSelect     = document.getElementById('layer-select');
const layerInfo       = document.getElementById('layer-info');
const bboxDisplay     = document.getElementById('bbox-display');
const formatSelect    = document.getElementById('format-select');
const memorySlider    = document.getElementById('memory-slider');
const memoryValue     = document.getElementById('memory-value');
const downloadBtn     = document.getElementById('download-btn');
const cancelBtn       = document.getElementById('cancel-btn');
const progressContainer = document.getElementById('progress-container');
const downloadInfo    = document.getElementById('download-info');
const progressBar     = document.getElementById('progress-bar');
const statusText      = document.getElementById('status-text');
const panelToggle     = document.getElementById('panel-toggle');
const panel           = document.getElementById('panel');
const extentsCheckbox = document.getElementById('show-extents');
const extentsStatus   = document.getElementById('extents-status');
const flattenStructsCheckbox = document.getElementById('flatten-structs');
const loadingIndicator = document.getElementById('loading-indicator');
const dataRelease     = document.getElementById('data-release');

// Populate format dropdown
for (const [value, { label, ext }] of Object.entries(FORMAT_OPTIONS)) {
  const opt = document.createElement('option');
  opt.value = value;
  opt.textContent = `${label} (${ext})`;
  formatSelect.appendChild(opt);
}

// --- URL state (layer stored in hash alongside MapLibre's map= param) ---

function getHashParams() {
  return new URLSearchParams(window.location.hash.substring(1));
}

function setHashParam(key, value) {
  const params = getHashParams();
  params.set(key, value);
  const newHash = '#' + params.toString().replaceAll('%2F', '/');
  window.history.replaceState(null, '', `${window.location.pathname}${window.location.search}${newHash}`);
}

// --- Map initialization (Carto Dark Matter) ---

const pmtilesProtocol = new pmtiles.Protocol();
maplibregl.addProtocol('pmtiles', pmtilesProtocol.tile);
registerCorrectionProtocol(maplibregl);

const map = new maplibregl.Map({
  container: 'map',
  hash: 'map',
  style: {
    version: 8,
    glyphs: 'https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf',
    sources: {
      'carto-dark': {
        type: 'raster',
        tiles: [
          'ibc://https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
          'ibc://https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
          'ibc://https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
        ],
        tileSize: 256,
        maxzoom: 20,
        attribution: '&copy; <a href="https://carto.com/attributions">CARTO</a> &copy; <a href="https://openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      },
    },
    layers: [
      { id: 'carto-dark', type: 'raster', source: 'carto-dark' },
    ],
  },
  center: [0, 20],
  zoom: 2,
  attributionControl: false,
});

map.addControl(new maplibregl.AttributionControl(), 'bottom-left');
map.addControl(new maplibregl.NavigationControl({ showCompass: false }));

// --- Catalog data (populated asynchronously) ---

// Map: layerKey → { theme, name, collectionUrl, pmtilesUrl }
let layerMap = {};
let catalogReady = false;

// --- Extractor (lazy-initialized on first download) ---

let extractor = null;
let duckdbPromise = null;
const sourceResolver = new OvertureSourceResolver();
const bboxReader = new HyparquetBboxReader();

// Clean up orphaned OPFS files from previous sessions
GeoParquetExtractor.cleanupOrphanedFiles();

// --- Memory slider setup ---

memorySlider.min = String(MEMORY_MIN_MB);
memorySlider.max = String(getDeviceMaxMemoryMB());
memorySlider.step = String(MEMORY_STEP);
memorySlider.value = String(getDefaultMemoryLimitMB());

function updateMemoryDisplay() {
  memoryValue.textContent = formatMemory(parseInt(memorySlider.value));
}
updateMemoryDisplay();
memorySlider.addEventListener('input', updateMemoryDisplay);

// --- Bbox display ---

function updateBbox() {
  const b = map.getBounds();
  const w = b.getWest().toFixed(5);
  const s = b.getSouth().toFixed(5);
  const e = b.getEast().toFixed(5);
  const n = b.getNorth().toFixed(5);
  bboxDisplay.innerHTML =
    `<span class="bbox-w">${w}</span>, <span class="bbox-s">${s}</span> → ` +
    `<span class="bbox-e">${e}</span>, <span class="bbox-n">${n}</span>`;
}
updateBbox();
map.on('moveend', updateBbox);

// --- Panel toggle ---

const isMobile = () => window.matchMedia('(max-width: 600px)').matches;

function updateToggleIcon() {
  const collapsed = panel.classList.contains('collapsed');
  panelToggle.textContent = isMobile()
    ? (collapsed ? '▲' : '▼')
    : (collapsed ? '◀' : '▶');
}

panelToggle.addEventListener('click', () => {
  panel.classList.toggle('collapsed');
  updateToggleIcon();
  setTimeout(() => map.resize(), 300);
});

window.matchMedia('(max-width: 600px)').addEventListener('change', updateToggleIcon);
updateToggleIcon();

// --- Layer info display ---

function updateLayerInfo() {
  const layer = layerMap[layerSelect.value];
  if (!layer) {
    layerInfo.innerHTML = '';
    return;
  }
  let html = `Theme: <b>${layer.theme}</b> · Type: <b>${layer.name}</b>`;
  if (layer.totalFeatures) {
    html += ` · Records: <b>${layer.totalFeatures.toLocaleString()}</b>`;
  }
  if (layer.license) {
    const attrUrl = `https://docs.overturemaps.org/attribution/#${layer.theme}`;
    html += ` · License: <a href="${attrUrl}" target="_blank" rel="noopener" title="See full attribution details">${layer.license}</a>`;
  }
  layerInfo.innerHTML = html;
}

// --- Load STAC catalog and populate layers ---

async function loadCatalog() {
  try {
    const { release, themes } = await resolveStacCatalog();

    dataRelease.textContent = `Release: ${release}`;

    // Build layer map and populate dropdown
    layerSelect.innerHTML = '';
    for (const theme of themes) {
      const group = document.createElement('optgroup');
      group.label = theme.theme.charAt(0).toUpperCase() + theme.theme.slice(1);

      for (const layer of theme.layers) {
        const key = `${theme.theme}/${layer.name}`;
        layerMap[key] = {
          theme: theme.theme,
          name: layer.name,
          collectionUrl: layer.collectionUrl,
          pmtilesUrl: theme.pmtilesUrl,
          license: layer.license,
          totalFeatures: layer.totalFeatures,
          description: layer.description,
          columns: layer.columns,
        };

        const opt = document.createElement('option');
        opt.value = key;
        opt.textContent = layer.name.replace(/_/g, ' ');
        group.appendChild(opt);
      }
      layerSelect.appendChild(group);
    }

    // Restore layer from URL hash
    const initialLayer = getHashParams().get('layer');
    if (initialLayer && layerMap[initialLayer]) {
      layerSelect.value = initialLayer;
    }
    setHashParam('layer', layerSelect.value);

    layerSelect.disabled = false;
    downloadBtn.disabled = false;
    catalogReady = true;

    updateLayerInfo();

  } catch (error) {
    console.error('Failed to load STAC catalog:', error);
    layerSelect.innerHTML = '<option value="">Error loading catalog</option>';
  } finally {
    loadingIndicator.classList.add('hidden');
  }
}

loadCatalog();

layerSelect.addEventListener('change', () => {
  updateLayerInfo();
  setHashParam('layer', layerSelect.value);
  // Reset extents when layer changes
  if (extentLoading) cancelExtentFetch();
  removeAllExtents();
  extentsCheckbox.checked = false;
  extentsStatus.textContent = '';
});

// --- Download ---

function setDownloading(active) {
  downloadBtn.disabled = active;
  cancelBtn.style.display = active ? 'inline-block' : 'none';
  progressContainer.style.display = active ? 'block' : 'none';
  layerSelect.disabled = active;
  formatSelect.disabled = active;
  memorySlider.disabled = active;
  flattenStructsCheckbox.disabled = active;
}

downloadBtn.addEventListener('click', async () => {
  const layer = layerMap[layerSelect.value];
  if (!layer) return;

  const format = formatSelect.value;
  const b = map.getBounds();
  const bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
  const memMB = parseInt(memorySlider.value);

  setDownloading(true);
  progressBar.style.width = '0%';

  const flattenStructs = flattenStructsCheckbox.checked;
  const bboxStr = `${bbox[0].toFixed(4)}, ${bbox[1].toFixed(4)} → ${bbox[2].toFixed(4)}, ${bbox[3].toFixed(4)}`;
  downloadInfo.innerHTML =
    `<b>${layer.theme} / ${layer.name}</b><br>` +
    `<span class="info-detail">Format: ${FORMAT_OPTIONS[format]?.label || format}</span><br>` +
    `<span class="info-detail">Bbox: ${bboxStr}</span><br>` +
    `<span class="info-detail">Memory: ${formatMemory(memMB)}</span><br>` +
    `<span class="info-detail">Flatten structs: ${flattenStructs ? 'Yes' : 'No'}</span>`;

  const onProgress = (pct) => { progressBar.style.width = `${pct}%`; };
  const onStatus = (msg) => {
    statusText.textContent = msg;
    statusText.classList.remove('error');
  };

  try {
    // Lazy-init DuckDB and extractor on first download
    if (!extractor) {
      onStatus('Initializing DuckDB WASM...');
      if (!duckdbPromise) duckdbPromise = initDuckDB(DUCKDB_DIST);
      const duckdb = await duckdbPromise;
      extractor = new GeoParquetExtractor({
        duckdb,
        sourceResolver,
        bboxReader,
        gpkgWorkerUrl: GPKG_WORKER_URL,
      });
    }

    const formatHandler = await extractor.prepare({
      sourceUrl: layer.collectionUrl,
      bbox,
      format,
      memoryLimitMB: memMB,
      flattenStructs,
      onProgress,
      onStatus,
    });

    // Check storage availability
    const browserUsage = formatHandler.getExpectedBrowserStorageUsage();
    const { usage, quota } = await getStorageEstimate();
    const available = quota - usage;

    onStatus(
      `Browser storage — expected: ${formatSize(browserUsage)}, available: ${formatSize(available)}`
    );

    if (browserUsage > available) {
      const totalDisk = formatHandler.getTotalExpectedDiskUsage();
      const msg =
        `Expected browser storage usage (${formatSize(browserUsage)}) exceeds available browser storage (${formatSize(available)}).\n` +
        `Total disk usage: ${formatSize(totalDisk)}.\nContinue anyway?`;
      if (!confirm(msg)) {
        setDownloading(false);
        statusText.textContent = 'Cancelled';
        setTimeout(() => { statusText.textContent = ''; downloadInfo.innerHTML = ''; }, 2000);
        return;
      }
    }

    const formatWarning = formatHandler.getFormatWarning?.();
    if (formatWarning) {
      if (formatWarning.isBlocking) {
        alert(formatWarning.message);
        setDownloading(false);
        statusText.textContent = 'Cancelled';
        setTimeout(() => { statusText.textContent = ''; downloadInfo.innerHTML = ''; }, 2000);
        return;
      }
      if (!confirm(formatWarning.message + '\n\nContinue anyway?')) {
        setDownloading(false);
        statusText.textContent = 'Cancelled';
        setTimeout(() => { statusText.textContent = ''; downloadInfo.innerHTML = ''; }, 2000);
        return;
      }
    }

    const baseName = GeoParquetExtractor.getDownloadBaseName(`overture_${layer.theme}_${layer.name}`, bbox);
    await extractor.download(formatHandler, { baseName, onProgress, onStatus });

    statusText.textContent = 'Complete!';
    setTimeout(() => {
      setDownloading(false);
      statusText.textContent = '';
      downloadInfo.innerHTML = '';
      progressBar.style.width = '0%';
    }, 2500);

  } catch (error) {
    if (error.name !== 'AbortError') {
      console.error('Download failed:', error);
      statusText.textContent = `Error: ${error.message}`;
      statusText.classList.add('error');
    } else {
      statusText.textContent = 'Cancelled';
    }
    setTimeout(() => {
      setDownloading(false);
      statusText.textContent = '';
      statusText.classList.remove('error');
      downloadInfo.innerHTML = '';
      progressBar.style.width = '0%';
    }, 3000);
  }
});

cancelBtn.addEventListener('click', () => {
  extractor?.cancel();
  statusText.textContent = 'Cancelling after current operation…';
});

// --- Extent visualization ---

const EXTENT_CONFIGS = {
  data: {
    sourceId: 'data-extents',
    labelSourceId: 'data-extents-labels-src',
    fillLayer: 'data-extents-fill',
    lineLayer: 'data-extents-line',
    labelLayer: 'data-extents-labels',
    fillColor: 'rgba(255, 152, 0, 0.15)',
    fillHoverColor: 'rgba(255, 152, 0, 0.4)',
    lineColor: 'rgba(255, 152, 0, 0.8)',
    lineHoverColor: 'rgba(255, 200, 0, 1)',
    textColor: '#FF9800',
  },
  rg: {
    sourceId: 'rg-extents',
    labelSourceId: 'rg-extents-labels-src',
    fillLayer: 'rg-extents-fill',
    lineLayer: 'rg-extents-line',
    labelLayer: 'rg-extents-labels',
    fillColor: 'rgba(0, 188, 212, 0.10)',
    fillHoverColor: 'rgba(0, 188, 212, 0.30)',
    lineColor: 'rgba(0, 188, 212, 0.7)',
    lineHoverColor: 'rgba(0, 230, 255, 1)',
    textColor: '#00BCD4',
  },
};

let extentData = null;
let extentDuckdb = null;
let extentDuckdbPromise = null;
let extentLoading = false;
const extentHoverHandlers = [];
const extentHoveredFeatures = new Map();

function extentsToGeoJSON(extents, fileMeta) {
  const emptyFC = { type: 'FeatureCollection', features: [] };
  if (!extents) return { polygons: emptyFC, labelPoints: emptyFC };
  const polyFeatures = [];
  const labelFeatures = [];
  for (const [name, bbox] of Object.entries(extents)) {
    const [minx, miny, maxx, maxy] = bbox;
    let label = name.includes(':') ? name.split(':').at(-1).replace(/^rg_/, '') : name;
    const meta = fileMeta?.[name];
    if (meta?.numRows) {
      label += ` (${meta.numRows.toLocaleString()})`;
    }
    polyFeatures.push({
      type: 'Feature',
      properties: { name, label },
      geometry: {
        type: 'Polygon',
        coordinates: [[[minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny]]],
      },
    });
    labelFeatures.push({
      type: 'Feature',
      properties: { label },
      geometry: { type: 'Point', coordinates: [minx, maxy] },
    });
  }
  return {
    polygons: { type: 'FeatureCollection', features: polyFeatures },
    labelPoints: { type: 'FeatureCollection', features: labelFeatures },
  };
}

function flattenRgExtents(rgExtents) {
  if (!rgExtents) return null;

  const flat = {};
  for (const [partitionId, groups] of Object.entries(rgExtents)) {
    if (!groups) continue;
    for (const [rgKey, bbox] of Object.entries(groups)) {
      flat[`${partitionId}:${rgKey}`] = bbox;
    }
  }

  return Object.keys(flat).length ? flat : null;
}

function addExtentLayer(cfg, extents, fileMeta) {
  const { polygons, labelPoints } = extentsToGeoJSON(extents, fileMeta);
  map.addSource(cfg.sourceId, { type: 'geojson', data: polygons, generateId: true });
  map.addLayer({
    id: cfg.fillLayer, type: 'fill', source: cfg.sourceId,
    paint: {
      'fill-color': ['case', ['boolean', ['feature-state', 'hover'], false],
        cfg.fillHoverColor, cfg.fillColor],
    },
  });
  map.addLayer({
    id: cfg.lineLayer, type: 'line', source: cfg.sourceId,
    paint: {
      'line-color': ['case', ['boolean', ['feature-state', 'hover'], false],
        cfg.lineHoverColor, cfg.lineColor],
      'line-width': ['case', ['boolean', ['feature-state', 'hover'], false], 2.5, 1.5],
    },
  });
  if (labelPoints.features.length > 1) {
    map.addSource(cfg.labelSourceId, { type: 'geojson', data: labelPoints });
    map.addLayer({
      id: cfg.labelLayer, type: 'symbol', source: cfg.labelSourceId,
      layout: {
        'text-field': ['get', 'label'],
        'text-size': 11,
        'text-anchor': 'top-left',
        'text-offset': [0.3, 0.3],
        'text-allow-overlap': false,
        'text-ignore-placement': false,
        'text-font': ['Open Sans Semibold'],
      },
      paint: {
        'text-color': cfg.textColor,
        'text-halo-color': 'rgba(0, 0, 0, 0.7)',
        'text-halo-width': 1,
      },
    });
  }
  addExtentHoverHandlers(cfg);
}

function removeExtentLayer(cfg) {
  for (const layer of [cfg.labelLayer, cfg.lineLayer, cfg.fillLayer]) {
    if (map.getLayer(layer)) map.removeLayer(layer);
  }
  for (const src of [cfg.labelSourceId, cfg.sourceId]) {
    if (map.getSource(src)) map.removeSource(src);
  }
}

function addExtentHoverHandlers(cfg) {
  const onMove = (e) => {
    const features = map.queryRenderedFeatures(e.point, { layers: [cfg.fillLayer] });
    const prevIds = extentHoveredFeatures.get(cfg.sourceId) || new Set();
    const nextIds = new Set(features.map(f => f.id));
    for (const id of prevIds) {
      if (!nextIds.has(id)) map.setFeatureState({ source: cfg.sourceId, id }, { hover: false });
    }
    for (const id of nextIds) {
      if (!prevIds.has(id)) map.setFeatureState({ source: cfg.sourceId, id }, { hover: true });
    }
    extentHoveredFeatures.set(cfg.sourceId, nextIds);
  };
  const onLeave = () => {
    const prevIds = extentHoveredFeatures.get(cfg.sourceId);
    if (prevIds) {
      for (const id of prevIds) map.setFeatureState({ source: cfg.sourceId, id }, { hover: false });
      extentHoveredFeatures.delete(cfg.sourceId);
    }
  };
  map.on('mousemove', cfg.fillLayer, onMove);
  map.on('mouseleave', cfg.fillLayer, onLeave);
  extentHoverHandlers.push({ layer: cfg.fillLayer, onMove, onLeave });
}

function removeAllExtents() {
  for (const { layer, onMove, onLeave } of extentHoverHandlers) {
    map.off('mousemove', layer, onMove);
    map.off('mouseleave', layer, onLeave);
  }
  extentHoverHandlers.length = 0;
  extentHoveredFeatures.clear();
  for (const cfg of Object.values(EXTENT_CONFIGS)) {
    removeExtentLayer(cfg);
  }
}

function cancelExtentFetch() {
  extentData?.cancel?.();
  extentDuckdb = null;
  extentDuckdbPromise = null;
  extentLoading = false;
  extentsCheckbox.disabled = false;
  extentsStatus.textContent = '';
}

async function showExtents() {
  removeAllExtents();
  extentsStatus.textContent = 'Loading…';
  extentLoading = true;

  try {
    if (!map.isStyleLoaded()) {
      await new Promise(resolve => map.once('load', resolve));
    }

    if (!extentDuckdbPromise) extentDuckdbPromise = initDuckDB(DUCKDB_DIST);
    const duckdb = await extentDuckdbPromise;
    extentDuckdb = duckdb;
    if (!extentData) {
      extentData = new ExtentData({ sourceResolver, bboxReader });
    }
    extentData.setDuckDB(duckdb);

    const layer = layerMap[layerSelect.value];
    if (!layer) return;

    const { dataExtents, rgExtents } = await extentData.fetchExtents({
      sourceUrl: layer.collectionUrl,
      bboxColumn: 'bbox',
      onStatus: (msg) => { extentsStatus.textContent = msg; },
    });

    if (!extentsCheckbox.checked) return;

    // Build file metadata lookup (numRows per partition) from cached resolver data
    const { files } = await sourceResolver.resolve(layer.collectionUrl);
    const fileMeta = {};
    for (const f of files) {
      if (f.numRows) fileMeta[f.id] = { numRows: f.numRows };
    }

    const flatRgExtents = flattenRgExtents(rgExtents);
    if (flatRgExtents) addExtentLayer(EXTENT_CONFIGS.rg, flatRgExtents);
    if (dataExtents) addExtentLayer(EXTENT_CONFIGS.data, dataExtents, fileMeta);

  } catch (error) {
    if (error.name === 'AbortError') return;
    console.error('Failed to show extents:', error);
    extentsStatus.textContent = 'Error loading extents';
  } finally {
    extentLoading = false;
    extentsStatus.textContent = '';
  }
}

extentsCheckbox.addEventListener('change', async () => {
  if (extentsCheckbox.checked) {
    await showExtents();
  } else {
    if (extentLoading) cancelExtentFetch();
    removeAllExtents();
    extentsStatus.textContent = '';
  }
});
