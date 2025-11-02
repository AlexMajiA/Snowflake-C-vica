--CLONAR TABLA--
USE DEV_CURSO_DB_ALUMNO_03 CREATE TABLE ADDRESSES_CLONADO CLONE DEV_CURSO_DB_ALUMNO_03.BRONZE.ADDRESSES
SELECT
    *
FROM
    addresses MINUS
SELECT
    *
FROM
    addresses_clonado --CLONADO SCHEMA
    CREATE SCHEMA SCHEMA_CLONADO CLONE DEV_CURSO_DB_ALUMNO_03.BRONZE USE SCHEMA bronze;
INSERT INTO
    addresses
VALUES(
        '10',
        '75074',
        'United States',
        '100 Sauthoff Trail',
        'Texas'
    );
SELECT
    *
FROM
    addresses;
DELETE FROM
    addresses
WHERE
    address_id = '10';
SELECT
    *
FROM
    addresses;
SELECT
    *
FROM
    addresses AT (OFFSET => -60 * 2);
-- También se puede hacer de esta forma, la cual le resta dos minutos al tiempo actual:
SELECT
    *
FROM
    addresses AT (
        TIMESTAMP => TIMESTAMPADD(MINUTE, -2, CURRENT_TIMESTAMP())
    );
CREATE
    OR REPLACE TABLE addresses_clone CLONE addresses at (
        TIMESTAMP => TIMESTAMPADD(MINUTE, -5, CURRENT_TIMESTAMP())
    );
DROP TABLE addresses;
SELECT
    *
FROM
    addreses;
UNDROP TABLE addresses;
SELECT
    *
FROM
    addresses;
TRUNCATE TABLE addresses_clonado;
SELECT
    *
FROM
    addresses_clonado;
CREATE
    OR REPLACE TABLE addresses_restaurado AS (
        SELECT
            *
        FROM
            addresses_clonado BEFORE (
                STATEMENT => '01c00fa9-0107-7afc-0000-18550ed478fe'
            )
    );
SELECT
    *
FROM
    addresses_restaurado;
CREATE OR REPLACE DYNAMIC TABLE SILVER.DT_ORDERS 
TARGET_LAG = DOWNSTREAM
WAREHOUSE = WH_CURSO_DATA_ENGINEERING 
REFRESH_MODE = AUTO INITIALIZE = on_create AS
SELECT
    ORDER_ID::varchar() AS ORDER_ID,
    SHIPPING_SERVICE::varchar(20) AS SHIPPING_SERVICE,
    (replace(SHIPPING_COST, ',', '.'))::decimal AS SHIPPING_COST,
    ADDRESS_ID::varchar(50) AS ADDRESS_ID,
    CREATED_AT::timestamp_ntz AS CREATED_AT,
    IFNULL(promo_id, 'N/A') AS PROMO_NAME,
    ESTIMATED_DELIVERY_AT::timestamp_ntz AS ESTIMATED_DELIVERY_AT,
    (replace(ORDER_COST, ',', '.'))::decimal AS ORDER_COST,
    USER_ID::varchar(50) AS USER_ID,
    (replace(ORDER_TOTAL, ',', '.'))::decimal AS ORDER_TOTAL,
    DELIVERED_AT::timestamp_ntz AS DELIVERED_AT,
    TRACKING_ID::varchar(50) AS TRACKING_ID,
    STATUS::varchar(20) AS STATUS,
    TIMESTAMPDIFF(HOUR, created_at, delivered_at) AS DELIVERY_TIME_HOURS
FROM
    CURSO_DATA_ENGINEERING_TO_BE_CLONED.BRONZE.orders_hist QUALIFY (
        ROW_NUMBER() OVER (
            PARTITION BY ORDER_ID
            ORDER BY
                CREATED_AT DESC
        )
    ) = 1;

--CAPA GOLD--

CREATE OR REPLACE DYNAMIC TABLE GOLD.DT_ORDERS 
TARGET_LAG = '60 SECONDS' 
WAREHOUSE = WH_CURSO_DATA_ENGINEERING 
REFRESH_MODE = AUTO 
INITIALIZE = on_create 
AS
SELECT
    TO_DATE(CREATED_AT) AS CREATED_AT,
    STATUS,
    COUNT(DISTINCT ORDER_ID) AS NUM_ORDER_ID
FROM
    SILVER.DT_ORDERS
GROUP BY
    TO_DATE(CREATED_AT),
    STATUS;