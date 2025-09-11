-- Catálogo mínimo (SQLite/DuckDB) para indexar el lake
CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY,
  source TEXT NOT NULL,
  market TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  symbol TEXT NOT NULL,
  year INT NOT NULL,
  month INT NOT NULL,
  path TEXT NOT NULL,
  ts_min TIMESTAMP,
  ts_max TIMESTAMP,
  rows BIGINT,
  compression TEXT,
  version TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS levels_daily (
  id INTEGER PRIMARY KEY,
  market TEXT NOT NULL,
  symbol TEXT NOT NULL,
  year INT NOT NULL,
  month INT NOT NULL,
  path TEXT NOT NULL,
  date_min DATE,
  date_max DATE,
  rows INT,
  version TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
