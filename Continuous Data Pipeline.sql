USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE DATABASE AIRLINEDB;
USE DATABASE AIRLINEDB;

CREATE OR REPLACE SCHEMA AIRLINESCHEMA;
USE SCHEMA AIRLINESCHEMA;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=TRUE,
    FIELD_OPTIONALLY_ENCLOSED_BY='"',
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE STAGE AIRLINE_STAGE 
    FILE_FORMAT = CSV_FORMAT
    	DIRECTORY = ( ENABLE = true );

CREATE OR REPLACE TABLE FLIGHTS (
    passenger_id INT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(100),
    age INT,
    nationality VARCHAR(100),
    airport_name VARCHAR(255),
    airport_country_code VARCHAR(10),
    country_name VARCHAR(100),
    airport_continent VARCHAR(100),
    continents VARCHAR(100),
    departure_date DATE,
    arrival_airport VARCHAR(255),
    pilot_name VARCHAR(255),
    flight_status VARCHAR(100)
);

CREATE OR REPLACE TASK LOAD_FLIGHTS
    WAREHOUSE = DATALOAD_WH
    SCHEDULE = '1 minutes'
AS
    COPY INTO FLIGHTS
    FROM @AIRLINE_STAGE
    PATTERN = '.*airline_[0-9].csv'
    FILE_FORMAT = CSV_FORMAT
    ON_ERROR = CONTINUE;

CREATE OR REPLACE TASK CLEAN_STAGE
    WAREHOUSE = DATALOAD_WH
    AFTER LOAD_FLIGHTS
    AS
        REMOVE @AIRLINE_STAGE
        PATTERN = '.*airline_[0-9].csv';

CREATE OR REPLACE STREAM FLIGHTS_STREAM
    ON TABLE FLIGHTS;

ALTER TASK CLEAN_STAGE RESUME;
ALTER TASK LOAD_FLIGHTS RESUME;

CREATE OR REPLACE TABLE DAILY_FLIGHT_STATS (
    id INT IDENTITY(1,1) PRIMARY KEY,
    date DATE,
    total_flights INT,
    on_time INT,
    delayed INT,
    cancelled INT
);

CREATE OR REPLACE TASK UPDATE_DAILY_STATS
    WAREHOUSE = COMPUTE_WH
    WHEN SYSTEM$STREAM_HAS_DATA('FLIGHTS_STREAM')
    AS
        MERGE INTO
            DAILY_FLIGHT_STATS AS target
        USING (
            SELECT
                departure_date AS Date,
                COUNT(*) AS total_flights,
                COUNT_IF(flight_status='On Time') AS on_time,
                COUNT_IF(flight_status='Delayed') AS delayed,
                COUNT_IF(flight_status='Cancelled') AS cancelled
            FROM
                FLIGHTS_STREAM
            GROUP BY
                departure_date
        ) AS source
        ON
            target.date = source.date
        WHEN MATCHED THEN
            UPDATE SET
                target.total_flights = target.total_flights + source.total_flights,
                target.on_time = target.on_time + source.on_time,
                target.delayed = target.delayed + source.delayed,
                target.cancelled = target.cancelled + source.cancelled
        WHEN NOT MATCHED THEN
            INSERT
                (date, total_flights, on_time, delayed, cancelled)
            VALUES (
                source.date,
                source.total_flights,
                source.on_time,
                source.delayed,
                source.cancelled
            );

ALTER TASK UPDATE_DAILY_STATS RESUME;

CREATE OR REPLACE DYNAMIC TABLE PASSENGER_DETAILS
    TARGET_LAG = '5 minutes'
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = incremental
    INITIALIZE = on_create
    AS
        SELECT
            passenger_id,
            CONCAT(first_name, ' ', last_name) AS name,
            gender,
            age,
            CASE
                WHEN age <18 THEN 'Under 18'
                WHEN age BETWEEN 18 AND 30 then '18-30'
                WHEN age BETWEEN 31 AND 50 then '31-50'
                WHEN age BETWEEN 51 AND 70 then '51-70'
                ELSE 'Over 70'
            END AS age_group,
            nationality
        FROM
            FLIGHTS;