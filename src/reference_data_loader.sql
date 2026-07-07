-- Date dimension derived from TRANS_MAIN.accounting_date.
-- Spans whole calendar years so time-intelligence (WoW/YoY) is contiguous.
CREATE OR REPLACE TABLE chpk_curated.propane.dim_date AS
WITH 
calendar AS (
    SELECT explode(sequence(DATE'2015-01-01', DATE'2030-12-31', interval 1 day)) AS full_date

)
SELECT
    full_date,                                                        -- DATE  (relate to the fact)
    CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
    year(full_date)                                 AS year,
    quarter(full_date)                              AS quarter,
    concat('Q', quarter(full_date))                 AS quarter_name,
    month(full_date)                                AS month_num,
    date_format(full_date, 'MMMM')                  AS month_name,
    date_format(full_date, 'MMM')                   AS month_short,
    concat(year(full_date), '-', lpad(month(full_date), 2, '0')) AS year_month,
    day(full_date)                                  AS day_of_month,
    dayofweek(full_date)                            AS day_of_week,   -- 1=Sun .. 7=Sat
    date_format(full_date, 'EEEE')                  AS day_name,
    weekofyear(full_date)                           AS iso_week,
    CAST(date_trunc('week', full_date) AS DATE)     AS week_start,    -- Monday (Spark)
    CASE WHEN dayofweek(full_date) IN (1,7) THEN true ELSE false END AS is_weekend
FROM calendar;
