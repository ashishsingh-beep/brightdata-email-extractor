-- Migration Script: Add query column to snapshot_table
-- Date: 2025-11-25
-- Purpose: Store array of queries associated with each snapshot_id

-- Step 1: Add the query column as TEXT[] (PostgreSQL array)
ALTER TABLE snapshot_table 
ADD COLUMN IF NOT EXISTS query TEXT[] DEFAULT '{}';

-- Step 2: Add index for better query performance
CREATE INDEX IF NOT EXISTS idx_snapshot_query 
ON snapshot_table USING GIN (query);

-- Step 3: Add comment for documentation
COMMENT ON COLUMN snapshot_table.query IS 'Array of search queries processed in this snapshot (batch of 2)';

-- Verification Query: Check the updated schema
-- SELECT column_name, data_type, is_nullable, column_default
-- FROM information_schema.columns
-- WHERE table_name = 'snapshot_table'
-- ORDER BY ordinal_position;

-- Sample Query: View snapshots with their queries
-- SELECT snapshot_id, query, processed, created_at
-- FROM snapshot_table
-- ORDER BY created_at DESC
-- LIMIT 10;

-- Sample Query: Find snapshots containing a specific query
-- SELECT snapshot_id, query, created_at
-- FROM snapshot_table
-- WHERE 'pizza restaurants near me' = ANY(query);

-- Sample Query: Count queries per snapshot
-- SELECT snapshot_id, array_length(query, 1) as query_count, created_at
-- FROM snapshot_table
-- WHERE array_length(query, 1) IS NOT NULL
-- ORDER BY created_at DESC;
