-- 数据完整xing
WITH date_sequence AS
         (SELECT SEQUENCE(DATE '2024-01-01', DATE '2024-06-30', INTERVAL '1' DAY) AS dates),
     date_table as
         (SELECT date FROM date_sequence CROSS JOIN UNNEST(dates) AS t(date))

SELECT ds.date
FROM date_table ds
WHERE NOT EXISTS (SELECT 1
                  FROM dws_user_daily_di dt
                  WHERE ds.date = dt.date)
ORDER BY ds.date;