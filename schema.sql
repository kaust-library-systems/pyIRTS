-- SQLite Schema for IRTS Harvest System
-- This schema maintains compatibility with the original MySQL IRTS database structure

-- Table 1: metadata
-- Stores standardized metadata in Dublin Core format with version control
CREATE TABLE IF NOT EXISTS metadata (
    rowID INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    idInSource TEXT NOT NULL,
    parentRowID INTEGER,
    field TEXT NOT NULL,
    place INTEGER NOT NULL DEFAULT 0,
    value TEXT,
    added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted TIMESTAMP,
    replacedByRowID INTEGER,
    FOREIGN KEY (parentRowID) REFERENCES metadata(rowID),
    FOREIGN KEY (replacedByRowID) REFERENCES metadata(rowID)
);

-- Indexes for metadata table (critical for performance)
CREATE INDEX IF NOT EXISTS idx_metadata_source_id ON metadata(source, idInSource);
CREATE INDEX IF NOT EXISTS idx_metadata_field ON metadata(field);
CREATE INDEX IF NOT EXISTS idx_metadata_deleted ON metadata(deleted);
CREATE INDEX IF NOT EXISTS idx_metadata_parent ON metadata(parentRowID);
CREATE INDEX IF NOT EXISTS idx_metadata_value ON metadata(value);

-- Table 2: sourceData
-- Stores raw XML/JSON from external sources with version control
CREATE TABLE IF NOT EXISTS sourceData (
    rowID INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    idInSource TEXT NOT NULL,
    sourceData TEXT,
    format TEXT CHECK(format IN ('XML', 'JSON')),
    added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted TIMESTAMP,
    replacedByRowID INTEGER,
    FOREIGN KEY (replacedByRowID) REFERENCES sourceData(rowID)
);

-- Indexes for sourceData table
CREATE INDEX IF NOT EXISTS idx_sourcedata_source_id ON sourceData(source, idInSource);
CREATE INDEX IF NOT EXISTS idx_sourcedata_deleted ON sourceData(deleted);
CREATE INDEX IF NOT EXISTS idx_sourcedata_added ON sourceData(added);

-- Table 3: mappings
-- Maps source-specific field names to standardized Dublin Core fields
CREATE TABLE IF NOT EXISTS mappings (
    rowID INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    sourceField TEXT NOT NULL,
    parentFieldInSource TEXT DEFAULT '',
    standardField TEXT NOT NULL,
    UNIQUE(source, sourceField, parentFieldInSource)
);

-- Index for mappings table
CREATE INDEX IF NOT EXISTS idx_mappings_lookup ON mappings(source, parentFieldInSource, sourceField);

-- Table 4: transformations
-- Defines value transformation rules applied to metadata
CREATE TABLE IF NOT EXISTS transformations (
    rowID INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    field TEXT NOT NULL,
    transformationType TEXT NOT NULL,
    transformationParameter TEXT,
    transformationValue TEXT,
    priority INTEGER DEFAULT 0
);

-- Index for transformations table
CREATE INDEX IF NOT EXISTS idx_transformations_lookup ON transformations(source, field, priority);

-- Table 5: messages
-- Stores harvest logs and reports for auditing
CREATE TABLE IF NOT EXISTS messages (
    rowID INTEGER PRIMARY KEY AUTOINCREMENT,
    process TEXT,
    type TEXT,
    message TEXT,
    added TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for messages table
CREATE INDEX IF NOT EXISTS idx_messages_process ON messages(process, type);
CREATE INDEX IF NOT EXISTS idx_messages_added ON messages(added);
