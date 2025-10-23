# Snowflake_Pipeline_Project

Pipeline created for Codecademy Snowflake course using SQL. Pipeline uses internal stage where .csv files are added.
Pipeline creates
- table FLIGHTS and loads data from stage to table with scheduled task 
- dynamic table PASSENGER_DETAILS from table FLIGHTS
- table DAILY_FLIGHT_STATS is created from table FLIGHTS using stream and triggered task

<img width="1910" height="1073" alt="image" src="https://github.com/user-attachments/assets/0d0de537-eac8-4a83-a54e-5212b94c90bf" />
