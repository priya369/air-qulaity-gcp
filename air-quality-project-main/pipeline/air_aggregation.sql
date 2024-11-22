-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE TABLE valid-verbena-437709-h5.air_quality.air_agg_data AS
SELECT 
    event_date,
    city,
    ROUND(AVG(pm10), 2) AS avg_pm10,
    ROUND(AVG(pm2_5), 2) AS avg_pm2_5,
    ROUND(AVG(carbon_monoxide), 2) AS avg_dust,
    ROUND(AVG(carbon_dioxide), 2) AS avg_uv_index,
    ROUND(AVG(nitrogen_dioxide), 2) AS avg_uv_index_clear_sky,
    ROUND(AVG(sulphur_dioxide), 2) AS avg_ammonia,
    ROUND(AVG(ozone), 2) AS avg_alder_pollen,
    ROUND(AVG(dust), 2) AS avg_birch_pollen,
    ROUND(AVG(uv_index), 2) AS avg_grass_pollen,
    ROUND(AVG(uv_index_clear_sky), 2) AS avg_mugwort_pollen,
    ROUND(AVG(ammonia), 2) AS avg_olive_pollen,
    ROUND(AVG(methane), 2) AS avg_ragweed_pollen,
    ROUND(MIN(pm10), 2) AS min_pm10,
    ROUND(MIN(pm2_5), 2) AS min_pm2_5,
    ROUND(MIN(carbon_monoxide), 2) AS avg_dust,
    ROUND(MIN(carbon_dioxide), 2) AS avg_uv_index,
    ROUND(MIN(nitrogen_dioxide), 2) AS avg_uv_index_clear_sky,
    ROUND(MIN(sulphur_dioxide), 2) AS avg_ammonia,
    ROUND(MIN(ozone), 2) AS avg_alder_pollen,
    ROUND(MIN(dust), 2) AS avg_birch_pollen,
    ROUND(MIN(uv_index), 2) AS avg_grass_pollen,
    ROUND(MIN(uv_index_clear_sky), 2) AS avg_mugwort_pollen,
    ROUND(MIN(ammonia), 2) AS avg_olive_pollen,
    ROUND(MIN(methane), 2) AS avg_ragweed_pollen,
    ROUND(MAX(pm10), 2) AS max_pm10,
    ROUND(MAX(pm2_5), 2) AS max_pm2_5,
    ROUND(MAX(carbon_monoxide), 2) AS avg_dust,
    ROUND(MAX(carbon_dioxide), 2) AS avg_uv_index,
    ROUND(MAX(nitrogen_dioxide), 2) AS avg_uv_index_clear_sky,
    ROUND(MAX(sulphur_dioxide), 2) AS avg_ammonia,
    ROUND(MAX(ozone), 2) AS avg_alder_pollen,
    ROUND(MAX(dust), 2) AS avg_birch_pollen,
    ROUND(MAX(uv_index), 2) AS avg_grass_pollen,
    ROUND(MAX(uv_index_clear_sky), 2) AS avg_mugwort_pollen,
    ROUND(MAX(ammonia), 2) AS avg_olive_pollen,
    ROUND(MAX(methane), 2) AS avg_ragweed_pollen

FROM 
    valid-verbena-437709-h5.air_quality.airquality_partitioned
GROUP BY
    event_date, city;
