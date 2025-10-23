# Snowflake_Pipeline_Project

Pipeline created for Codecademy Snowflake course using SQL. Pipeline uses internal stage where .csv files are added. Data contains flight data.

**Pipeline creates**
- table FLIGHTS and loads data from stage to table with scheduled task 
- dynamic table PASSENGER_DETAILS from table FLIGHTS
- table DAILY_FLIGHT_STATS is created from table FLIGHTS using stream and triggered task

Also a dashboard based on the data is created.

**Pipeline architecture**
<img width="1910" height="1073" alt="image" src="https://github.com/user-attachments/assets/0d0de537-eac8-4a83-a54e-5212b94c90bf" />

**Dashboard**
<img width="1903" height="907" alt="image" src="https://github.com/user-attachments/assets/896132ee-9cd1-477c-8d71-76054fad20a8" />
