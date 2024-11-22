-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE TABLE valid-verbena-437709-h5.air_quality.combined_data AS
SELECT 
    w.event_date AS date,
    w.city,
    w.temperature_2m,
    w.relative_humidity_2m,
    w.precipitation,
    w.rain,
    w.showers,
    w.surface_pressure,
    w.cloud_cover,
    w.visibility,
    w.wind_speed_10m,
    w.wind_gusts_10m,
    a.pm10,
    a.pm2_5,
    a.carbon_monoxide,
    a.carbon_dioxide,
    a.nitrogen_dioxide,
    a.sulphur_dioxide,
    a.ozone,
    a.dust,
    a.uv_index,
    a.uv_index_clear_sky,
    a.ammonia,
    a.methane
FROM 
    valid-verbena-437709-h5.air_quality.weather_partitioned w
JOIN 
    valid-verbena-437709-h5.air_quality.airquality_partitioned a ON w.event_date = a.event_date AND w.city = a.city;
