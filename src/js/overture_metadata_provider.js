// Overture Maps source resolver
// Resolves STAC collection.json URLs to concrete parquet files with item-level bboxes.

import { SourceResolver } from 'geoparquet-extractor';

function resolveUrl(base, relative) {
  return new URL(relative, base).href;
}

export class OvertureSourceResolver extends SourceResolver {
  constructor() {
    super();
    // Per-item cache: itemUrl → { id, url, bbox }
    this._itemCache = new Map();
    // Collection structure cache: collectionUrl → { itemLinks }
    this._collectionStructureCache = new Map();
  }

  async _fetchCollectionItems(collectionUrl, signal, onStatus) {
    // Fetch and cache the collection structure (item link list)
    let itemLinks;
    if (this._collectionStructureCache.has(collectionUrl)) {
      itemLinks = this._collectionStructureCache.get(collectionUrl);
    } else {
      const resp = await fetch(collectionUrl, { signal });
      if (!resp.ok) throw new Error(`Failed to fetch collection: ${resp.status}`);
      const collection = await resp.json();
      itemLinks = collection.links.filter(link => link.rel === 'item');
      this._collectionStructureCache.set(collectionUrl, itemLinks);
    }

    const files = [];
    const uncachedLinks = [];
    for (const link of itemLinks) {
      const itemUrl = resolveUrl(collectionUrl, link.href);
      if (this._itemCache.has(itemUrl)) {
        const cached = this._itemCache.get(itemUrl);
        if (cached) files.push(cached);
      } else {
        uncachedLinks.push({ link, itemUrl });
      }
    }

    const BATCH_SIZE = 5;
    for (let i = 0; i < uncachedLinks.length; i += BATCH_SIZE) {
      signal?.throwIfAborted?.();
      onStatus?.(`Resolving files… ${files.length + Math.min(i + BATCH_SIZE, uncachedLinks.length)}/${itemLinks.length}`);
      const batch = uncachedLinks.slice(i, i + BATCH_SIZE);
      const items = await Promise.all(
        batch.map(async ({ itemUrl }) => {
          const itemResp = await fetch(itemUrl, { signal });
          if (!itemResp.ok) return { itemUrl, item: null };
          return { itemUrl, item: await itemResp.json() };
        })
      );

      for (const { itemUrl, item } of items) {
        if (!item) {
          this._itemCache.set(itemUrl, null);
          continue;
        }
        const awsAsset = item.assets?.aws;
        if (!awsAsset?.href) {
          this._itemCache.set(itemUrl, null);
          continue;
        }
        const resolved = {
          id: item.id,
          url: awsAsset.href,
          bbox: item.bbox || null,
          numRows: item.properties?.num_rows || null,
        };
        this._itemCache.set(itemUrl, resolved);
        files.push(resolved);
      }
    }

    return { files };
  }

  async resolve(sourceUrl, { bbox, signal, onStatus } = {}) {
    const { files } = await this._fetchCollectionItems(sourceUrl, signal, onStatus);
    if (!bbox) return { files };

    const [west, south, east, north] = bbox;
    return {
      files: files.filter(file => {
        if (!file.bbox || file.bbox.length < 4) return true;
        const [minx, miny, maxx, maxy] = file.bbox;
        return minx <= east && maxx >= west && miny <= north && maxy >= south;
      }),
    };
  }
}
