-- Migration script to add read_interval_ms column to devices table
-- Date: September 21, 2025
-- Description: Add configurable reading speed for RTU and TCP devices

-- Add the read_interval_ms column with default 1000ms (1 second)
ALTER TABLE devices 
ADD COLUMN read_interval_ms INT DEFAULT 1000 
COMMENT 'Reading interval in milliseconds (50-10000ms)';

-- Update existing devices to have the default reading speed
UPDATE devices SET read_interval_ms = 1000 WHERE read_interval_ms IS NULL;

-- Verify the migration
SELECT COUNT(*) as device_count, 
       AVG(read_interval_ms) as avg_interval,
       MIN(read_interval_ms) as min_interval,
       MAX(read_interval_ms) as max_interval
FROM devices;

-- Show sample devices with new column
SELECT id, name, protocol, read_interval_ms, timeout_ms
FROM devices 
LIMIT 5;