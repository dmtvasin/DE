
-- DDL создание таблицы

CREATE TABLE Operations (
 	id int,
 	loan_id int,
 	is_income_flg char(1), -- 1/0
 	type varchar(3), -- MD/INT/PEN/COM
 	created_at date,
 	amount int
);

-- Наполнение тестовыми данными

INSERT INTO Operations (id, loan_id, is_income_flg, type, created_at, amount)
VALUES
  (1, 1, '0', 'MD', '2023-08-21', 1000),
  (2, 2, '0', 'MD', '2023-08-21', 1000), 
  (3, 1, '1', 'MD', '2023-08-23', 70),  
  (4, 1, '1', 'INT', '2023-08-23', 30),
  (5, 2, '1', 'PEN', '2023-08-23', 100),
  (6, 1, '1', 'MD', '2023-08-25', 70),  
  (7, 1, '1', 'INT', '2023-08-25', 30),  
  (8, 2, '1', 'MD', '2023-08-25', 70),  
  (9, 2, '1', 'INT', '2023-08-25', 30),    
  (10, 2, '1', 'COM', '2023-08-25', 5),      
  (11, 1, '1', 'PEN', '2023-08-28', 15),
  (12, 2, '1', 'PEN', '2023-08-28', 15);

-- Запрос выводящий для каждого дня без пропусков, начиная с минимального значения Operations.created_at и заканчивая текущим днем
-- 1.Портфель = весь outcome по счету ОД - income по счету ОД
-- 2.Прибыль = income остальных типов операций

-- Определяем диапазон дат от минимальной даты операций до текущей даты   
WITH DateRange AS (
  SELECT 
  MIN(created_at) AS start_date,
  CURRENT_DATE AS end_date
  FROM Operations
),
-- Генерируем список всех дат в этом диапазоне
AllDates AS (
  SELECT 
  generate_series(start_date, end_date, interval '1 day')::date AS date
  FROM DateRange
),
DailyTotals AS (
  SELECT
  ad.date AS date,
  COALESCE(SUM(CASE WHEN o.type = 'MD' AND o.is_income_flg = '0' THEN o.amount ELSE 0 END), 0) - 
  COALESCE(SUM(CASE WHEN o.type = 'MD' AND o.is_income_flg = '1' THEN o.amount ELSE 0 END), 0) AS portfolio,
  COALESCE(SUM(CASE WHEN o.is_income_flg = '1' AND o.type != 'MD' THEN o.amount ELSE 0 END), 0) AS profit
  FROM AllDates ad
  LEFT JOIN Operations o ON ad.date = o.created_at
  GROUP BY ad.date
)
SELECT
date,
SUM(portfolio) OVER (ORDER BY date) AS portfolio, -- весь outcome по счету ОД 
profit, -- income остальных типов операций на определенную дату
SUM(profit) OVER (ORDER BY date) AS total_profit -- кумулятивная сумма income остальных типов операций
FROM DailyTotals
ORDER BY date;