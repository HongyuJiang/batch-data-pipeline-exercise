create_unfinished_orders_count_table_sql = """
CREATE TABLE IF NOT EXISTS unfinished_orders_count (
    count INTEGER,
    processed_time TIMESTAMP
);
truncate unfinished_orders_count;
"""

unfinished_orders_count_sql = """
WITH UOC AS (
    SELECT count(distinct(id)) AS order_num FROM stg_orders WHERE status = 'created' AND id not in 
    (SELECT distinct(id) FROM stg_orders WHERE status = 'completed')
)
INSERT INTO unfinished_orders_count(count, processed_time)
SELECT UOC.order_num AS count,
    '{{ ts }}' AS processed_time
FROM UOC
"""

create_orders_count_recent_two_years_quaterly_table_sql = """
CREATE TABLE IF NOT EXISTS orders_count_recent_two_years_quaterly (
    count INTEGER,
    quarter SMALLINT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    processed_time TIMESTAMP
);
truncate orders_count_recent_two_years_quaterly;
"""

orders_count_recent_two_years_quaterly_sql = """
WITH OCRTYQ AS (
    SELECT dim_dates.quarter AS quarter, count(distinct(stg_orders.id)) AS order_num,
    CAST('{{ ts }}' AS date) AS current_date,
    (current_date - INTERVAL '2 year') as start_time
    FROM stg_orders, dim_dates
    WHERE
    date(stg_orders.event_time) >= (current_date - INTERVAL '2 year') 
    AND
    dim_dates.datum = date(stg_orders.event_time)
    GROUP BY dim_dates.quarter
)
INSERT INTO orders_count_recent_two_years_quaterly(count, quarter, start_time, end_time, processed_time)
SELECT OCRTYQ.order_num AS count,
    OCRTYQ.quarter AS quarter,
    OCRTYQ.start_time AS start_time,
    '{{ ts }}' AS end_time,
    '{{ ts }}' AS processed_time
FROM OCRTYQ
"""

create_orders_count_recent_two_years_quaterly_category_table_sql = """
CREATE TABLE IF NOT EXISTS orders_count_recent_two_years_quaterly_category (
    count INTEGER,
    quarter SMALLINT,
    category VARCHAR,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    processed_time TIMESTAMP
);
truncate orders_count_recent_two_years_quaterly_category;
"""

orders_count_recent_two_years_quaterly_category_sql = """
WITH OCRTQC AS (
    SELECT dim_dates.quarter AS quarter, count(distinct(stg_orders.id)) AS order_num,
    (CAST('{{ ts }}' AS date) - INTERVAL '2 year') AS start_time,
    stg_products.category AS category
    FROM stg_orders, dim_dates, stg_products 
    WHERE
    date(stg_orders.event_time) >= (CAST('{{ ts }}' AS date) - INTERVAL '2 year')
    AND
    dim_dates.datum = date(stg_orders.event_time)
    AND
    stg_products.id = stg_orders.product_id
    GROUP BY dim_dates.quarter, stg_products.category
)
INSERT INTO orders_count_recent_two_years_quaterly_category(count, quarter, category, start_time, end_time, processed_time)
SELECT OCRTQC.order_num AS count,
        OCRTQC.quarter AS quarter,
        OCRTQC.category AS category,
        OCRTQC.start_time AS start_time,
        '{{ ts }}' AS end_time,
        '{{ ts }}' AS processed_time
FROM OCRTQC
"""

create_unfinished_orders_recent_month_count_table_sql = """
CREATE TABLE IF NOT EXISTS unfinished_orders_recent_month_count (
    count INTEGER,
    processed_time TIMESTAMP
);
truncate unfinished_orders_recent_month_count;
"""

unfinished_orders_recent_month_count_sql = """
WITH UORMC AS (
    SELECT count(distinct(id)) AS order_num FROM stg_orders WHERE status = 'created' AND 
    id not in 
        (SELECT distinct(id) FROM stg_orders WHERE status = 'completed')
    AND
    to_char(event_time, 'YYYY-MM') <> to_char(CAST('{{ ts }}' AS date), 'YYYY-MM')
)
INSERT INTO unfinished_orders_recent_month_count(count, processed_time)
SELECT UORMC.order_num AS count,
        '{{ ts }}' AS processed_time
FROM UORMC
"""

create_unfinished_orders_recent_two_years_monthly_count_table_sql = """
CREATE TABLE IF NOT EXISTS unfinished_orders_recent_two_years_monthly_count (
    count INTEGER,
    month VARCHAR,
    processed_time TIMESTAMP
);
truncate unfinished_orders_recent_two_years_monthly_count;
"""

unfinished_orders_recent_two_years_monthly_count_sql = """
WITH UORTYMC AS (
    SELECT count(id) as order_num, to_char(event_time, 'YYYY-MM') AS month 
    FROM stg_orders A 
    WHERE status = 'created' 
    AND id not in (SELECT id from stg_orders B WHERE status = 'completed' 
    AND EXTRACT( MONTH FROM A.event_time::date) = EXTRACT( MONTH FROM B.event_time::date))
    AND date(event_time) >= (CAST('{{ ts }}' AS date) - INTERVAL '2 year')
    GROUP BY month
)
INSERT INTO unfinished_orders_recent_two_years_monthly_count(count, month, processed_time)
SELECT UORTYMC.order_num AS count,
        UORTYMC.month AS month,
        '{{ ts }}' AS processed_time
FROM UORTYMC
"""