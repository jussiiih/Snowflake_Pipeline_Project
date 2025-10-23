USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE DATABASE AIRLINEDB;
USE DATABASE AIRLINEDB;

CREATE OR REPLACE SCHEMA AIRLINESCHEMA;
USE SCHEMA AIRLINESCHEMA;

CREATE OR REPLACE STAGE AIRLINE_STAGE 
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
    FROM (
        SELECT
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
        FROM
            '@"AIRLINEDB"."AIRLINESCHEMA"."AIRLINE_STAGE"'
    )
    FILE_FORMAT = (
        TYPE=CSV,
        SKIP_HEADER=1,
        FIELD_DELIMITER=',',
        TRIM_SPACE=TRUE,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        REPLACE_INVALID_CHARACTERS=TRUE,
        DATE_FORMAT=AUTO,
        TIME_FORMAT=AUTO,
        TIMESTAMP_FORMAT=AUTO
    )
    ON_ERROR=CONTINUE;

ALTER TASK LOAD_FLIGHTS RESUME;

CREATE OR REPLACE DYNAMIC TABLE PASSENGER_DETAILS
    TARGET_LAG = '1 minutes'
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = auto
    INITIALIZE = on_create
    AS
        SELECT
            passenger_id,
            first_name,
            last_name,
            gender,
            age,
            nationality
        FROM
            FLIGHTS;

CREATE OR REPLACE STREAM FLIGHTS_STREAM
    ON TABLE FLIGHTS;

CREATE OR REPLACE TABLE DAILY_FLIGHT_STATS (
    id INT IDENTITY(1,1) PRIMARY KEY,
    departure_date DATE,
    country_name VARCHAR(100),
    on_time INT,
    delayed INT,
    cancelled INT
);

CREATE OR REPLACE TASK UPDATE_DAILY_STATS
    WAREHOUSE = COMPUTE_WH
    WHEN SYSTEM$STREAM_HAS_DATA('FLIGHTS_STREAM')
    --SCHEDULE = '1 minutes'
    AS
    MERGE INTO
        DAILY_FLIGHT_STATS AS target
    USING (
        SELECT
            departure_date,
            country_name,
            COUNT_IF(flight_status='On Time') AS on_time,
            COUNT_IF(flight_status='Delayed') AS delayed,
            COUNT_IF(flight_status='Cancelled') AS cancelled
        FROM
            FLIGHTS_STREAM
        GROUP BY
            departure_date, country_name
    ) AS source
    ON
        target.departure_date = source.departure_date
        AND
        target.country_name = source.country_name
    WHEN MATCHED THEN
        UPDATE SET
            target.on_time = target.on_time + source.on_time,
            target.delayed = target.delayed + source.delayed,
            target.cancelled = target.cancelled + source.cancelled
    WHEN NOT MATCHED THEN
        INSERT
            (departure_date, country_name, on_time, delayed, cancelled)
        VALUES (
            source.departure_date,
            source.country_name,
            source.on_time,
            source.delayed,
            source.cancelled
        );

ALTER TASK UPDATE_DAILY_STATS RESUME;