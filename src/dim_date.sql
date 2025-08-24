-- Create table
CREATE OR REPLACE TABLE `thelook_ecommerce_dw.dim_date` AS
WITH calendar AS (
  SELECT day AS date_actual
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2016-01-01', DATE '2030-12-31', INTERVAL 1 DAY)) AS day
)
SELECT
  CAST(FORMAT_DATE('%Y%m%d', date_actual) AS INT64) AS date_key,
  date_actual,
  EXTRACT(DAY FROM date_actual) AS day_of_month,
  EXTRACT(MONTH FROM date_actual) AS month_number,
  EXTRACT(YEAR FROM date_actual) AS year,
  FORMAT_DATE('%A', date_actual) AS day_name,
  FORMAT_DATE('%B', date_actual) AS month_name,
  EXTRACT(QUARTER FROM date_actual) AS quarter,
  CONCAT('Q', CAST(EXTRACT(QUARTER FROM date_actual) AS STRING), '-', CAST(EXTRACT(YEAR FROM date_actual) AS STRING)) AS year_quarter,
  CASE WHEN FORMAT_DATE('%a', date_actual) IN ('Sat', 'Sun') THEN TRUE ELSE FALSE END AS is_weekend,
  EXTRACT(DAYOFWEEK FROM date_actual) AS day_of_week,         -- Sunday=1 to Saturday=7
  EXTRACT(DAYOFYEAR FROM date_actual) AS day_of_year,
  EXTRACT(WEEK FROM date_actual) AS week_of_year,
  DATE_TRUNC(date_actual, MONTH) AS first_day_of_month,
  DATE_SUB(DATE_TRUNC(DATE_ADD(date_actual, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) AS last_day_of_month,
  FORMAT_DATE('%Y-%m', date_actual) AS yyyy_mm,

  -- ✅ ISO-specific columns
  EXTRACT(ISOWEEK FROM date_actual) AS iso_week_of_year,
  EXTRACT(ISOYEAR FROM date_actual) AS iso_year,
  CONCAT(
    CAST(EXTRACT(ISOYEAR FROM date_actual) AS STRING),
    '-W',
    LPAD(CAST(EXTRACT(ISOWEEK FROM date_actual) AS STRING), 2, '0')
  ) AS iso_year_week
FROM calendar;
