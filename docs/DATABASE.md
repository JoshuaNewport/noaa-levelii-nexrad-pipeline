# Level II Index Database Documentation

The Level II processor uses a centralized SQLite database to manage metadata for all stored radar frames. This replaces the previous multi-JSON file approach, providing better concurrency, improved query performance, and lower memory overhead.

## Database Details

- **Filename**: `index.db`
- **Location**: Base storage directory (default: `./data/levelii/index.db`)
- **Table Name**: `levelii_frames` (Shared with sister programs using the same database file)

## Schema

```sql
CREATE TABLE levelii_frames (
    station TEXT,             -- 4-letter radar station ID (e.g., KTLX)
    product_code INTEGER,     -- Product code (default 0 for Level II)
    product_name TEXT,        -- Descriptive name/category (e.g., reflectivity, velocity)
    timestamp TEXT,           -- ISO-8601 formatted timestamp (e.g., 2024-05-20T12:30:00Z)
    filename TEXT,            -- Name of the data file on disk (e.g., 0.5.RDA, volumetric.RDA)
    PRIMARY KEY (station, product_name, timestamp, filename)
);
```

## Indexes

To ensure high performance for common lookups, the following indexes are maintained:

- `idx_levelii_station_product`: Optimized for lookups by station and product name.
- `idx_levelii_timestamp`: Optimized for temporal range queries.

## Volumetric Data

Volumetric datasets (containing multiple tilts in a single scan) are indexed with the special filename `volumetric.RDA`.

## Common Queries

### List all products for a station
```sql
SELECT DISTINCT product_name FROM levelii_frames WHERE station = 'KTLX';
```

### Get latest 10 frames for a specific product
```sql
SELECT timestamp, filename
FROM levelii_frames
WHERE station = 'KTLX' AND product_name = 'reflectivity'
ORDER BY timestamp DESC
LIMIT 10;
```

### Find frames within a time range
```sql
SELECT station, product_name, timestamp, filename
FROM levelii_frames
WHERE timestamp BETWEEN '2024-05-20T12:00:00Z' AND '2024-05-20T13:00:00Z'
ORDER BY timestamp ASC;
```

## Maintenance

The `FrameStorageManager` automatically handles:
- **Pruning**: When old frames are deleted from disk during cleanup, the corresponding records are purged from the database using `purge_old_records`.
- **Concurrency**: The database uses Write-Ahead Logging (`WAL`) mode to allow simultaneous reads while writing.
