# Overture Maps Extract

**🌐 [Live Site](https://ramseraph.github.io/overturemaps-extract/)**

A browser-based tool for extracting [Overture Maps](https://overturemaps.org/) data by bounding box. Discovers available datasets automatically from the [Overture STAC Catalog](https://stac.overturemaps.org/catalog.json) and reads GeoParquet files directly via HTTP range requests.

Built with [geoparquet-extractor](https://github.com/ramSeraph/geoparquet_extractor) — all processing happens client-side with no backend involved.

## Features

- **Auto-discovers** the latest Overture Maps release via STAC catalog traversal
- **Visual preview** of each theme using PMTiles overlays on a dark Carto basemap
- **Bounding box selection** — draw a rectangle on the map to define the extract area
- **Data extent visualization** — view partition extents and row-group-level bounding boxes
- **Client-side extraction** — reads only the relevant GeoParquet row groups over HTTP

## Available Themes

Themes and layers are resolved dynamically from the catalog. As of the latest release these include:

| Theme | Layers |
|-------|--------|
| Addresses | address |
| Base | infrastructure, land, land_cover, land_use, water |
| Buildings | building, building_part |
| Divisions | division, division_area, division_boundary |
| Places | place |
| Transportation | connector, segment |

## Output Formats

- **GeoJSON** (`.geojson`)
- **GeoJSONSeq** (`.geojsonl`) — newline-delimited
- **GeoParquet v1.1** (`.parquet`)
- **GeoParquet v2.0** (`.parquet`)
- **GeoPackage** (`.gpkg`) — with R-tree spatial index
- **CSV** (`.csv`) — with WKT geometry
- **Shapefile** (`.shp`)
- **KML** (`.kml`)
- **DXF** (`.dxf`)

## Data License

Overture Maps data is released under a combination of open licenses depending on the theme and source. See the [Overture Maps Attribution](https://docs.overturemaps.org/attribution/) page for full details.

| Theme | License |
|-------|---------|
| Buildings | [ODbL-1.0](https://opendatacommons.org/licenses/odbl/) |
| Transportation | [ODbL-1.0](https://opendatacommons.org/licenses/odbl/) |
| Base | [ODbL-1.0](https://opendatacommons.org/licenses/odbl/) (most layers), [CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/) (land_cover) |
| Divisions | [ODbL-1.0](https://opendatacommons.org/licenses/odbl/) |
| Places | [CDLA-Permissive-2.0](https://cdla.dev/permissive-2-0/), [ODbL-1.0](https://opendatacommons.org/licenses/odbl/) |
| Addresses | [Multiple Open Licenses](https://docs.overturemaps.org/attribution/#addresses) |

## How It Works

1. Fetches the [STAC root catalog](https://stac.overturemaps.org/catalog.json) to find the latest release
2. Traverses theme subcatalogs in parallel to discover layers and PMTiles URLs
3. For extraction, fetches the layer's STAC collection to locate GeoParquet partition files
4. Uses HTTP range requests to read only the Parquet row groups that intersect the selected bounding box
5. Converts and downloads the result in the chosen output format
