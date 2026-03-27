// STAC Catalog Resolver for Overture Maps
// Traverses the STAC catalog hierarchy to discover themes, layers, and PMTiles URLs.

const STAC_ROOT = 'https://stac.overturemaps.org/catalog.json';
const jsonCache = new Map();
let catalogCache = null;

function resolveUrl(base, relative) {
  return new URL(relative, base).href;
}

async function fetchJson(url) {
  if (!jsonCache.has(url)) {
    const pending = fetch(url)
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Failed to fetch STAC JSON: ${response.status}`);
        }
        return response.json();
      })
      .catch((error) => {
        jsonCache.delete(url);
        throw error;
      });
    jsonCache.set(url, pending);
  }

  return jsonCache.get(url);
}

/**
 * Fetches the root catalog, finds the latest release, then fetches all theme
 * subcatalogs in parallel. Returns structured metadata about themes, layers,
 * and pmtiles URLs.
 *
 * @returns {Promise<{release: string, themes: Object[]}>}
 */
export async function resolveStacCatalog() {
  if (!catalogCache) {
    catalogCache = (async () => {
      const root = await fetchJson(STAC_ROOT);

      const latestLink = root.links.find(l => l.latest === true && l.rel === 'child');
      if (!latestLink) throw new Error('No latest release found in STAC catalog');

      const releaseUrl = resolveUrl(STAC_ROOT, latestLink.href);
      const release = await fetchJson(releaseUrl);
      const releaseVersion = release['release:version'] || release.id;

      const themeLinks = release.links.filter(l => l.rel === 'child');
      const themeResults = await Promise.all(
        themeLinks.map(async (link) => {
          const themeUrl = resolveUrl(releaseUrl, link.href);
          const themeCatalog = await fetchJson(themeUrl);

          const pmtilesLink = themeCatalog.links.find(l => l.rel === 'pmtiles');
          const pmtilesUrl = pmtilesLink?.href || null;

          const layerLinks = themeCatalog.links.filter(l => l.rel === 'child');
          const layers = await Promise.all(layerLinks.map(async (ll) => {
            const collectionUrl = resolveUrl(themeUrl, ll.href);
            const parts = ll.href.split('/');
            const layerName = parts[parts.length - 2] || parts[parts.length - 1].replace('.json', '');

            let license = null;
            let totalFeatures = null;
            let description = null;
            let columns = null;
            try {
              const collection = await fetchJson(collectionUrl);
              license = collection.license || null;
              totalFeatures = collection.features || null;
              description = collection.description || null;
              columns = collection.summaries?.columns || null;
            } catch (e) {
              console.warn(`Failed to fetch collection metadata for ${layerName}:`, e);
            }

            return {
              name: layerName,
              collectionUrl,
              license,
              totalFeatures,
              description,
              columns,
            };
          }));

          return {
            theme: themeCatalog.id,
            pmtilesUrl,
            layers,
          };
        })
      );

      return {
        release: releaseVersion,
        themes: themeResults,
      };
    })().catch((error) => {
      catalogCache = null;
      throw error;
    });
  }

  return catalogCache;
}
