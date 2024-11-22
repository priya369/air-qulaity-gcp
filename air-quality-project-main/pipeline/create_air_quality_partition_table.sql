-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE OR REPLACE TABLE `valid-verbena-437709-h5.air_quality.airquality_partitioned`
    PARTITION BY DATE(event_date)
    CLUSTER BY city AS (
    SELECT * FROM `valid-verbena-437709-h5.air_quality.airquality`
    );