# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "343b9929-b995-462d-a058-4637a4ed6bac",
# META       "default_lakehouse_name": "dashboard_data",
# META       "default_lakehouse_workspace_id": "371caf18-1651-4320-b03d-ddd574e85e48",
# META       "known_lakehouses": [
# META         {
# META           "id": "343b9929-b995-462d-a058-4637a4ed6bac"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

%%sql

# CREATE TABLE aggregate_test AS
SELECT
    Date(period_first_depart),
    -- area_first_depart, 
    area_code_first_depart, 
    area_name_first_depart,
    train_number_merge,
    train_number,
    train_name,
    -- station_first_depart, 
    station_code_first_depart,
    station_code_last_arrival,
    -- distance_od,
    -- time_first_depart,
    -- time_last_arrival,
    train_local,
    train_class,
    train_priority,
    train_pso,
    train_perintis,
    sum(passenger) as total_passenger, 
    sum(amount) as total_amount
FROM
    dashboard_ticketing_aggregate_system_posko_raw
GROUP BY
    Date(period_first_depart),
    -- area_first_depart, 
    area_code_first_depart, 
    area_name_first_depart,
    train_number_merge,
    train_number,
    train_name,
    -- station_first_depart, 
    station_code_first_depart,
    station_code_last_arrival,
    -- distance_od,
    -- time_first_depart,
    -- time_last_arrival,
    train_local,
    train_class,
    train_priority,
    train_pso,
    train_perintis
    




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- CREATE TABLE aggregate_test AS
# MAGIC SELECT
# MAGIC     Date(period_first_depart),
# MAGIC     -- area_first_depart, 
# MAGIC     area_code_first_depart, 
# MAGIC     area_name_first_depart,
# MAGIC     train_number_merge,
# MAGIC     train_number,
# MAGIC     train_name,
# MAGIC     -- station_first_depart, 
# MAGIC     station_code_first_depart,
# MAGIC     station_code_last_arrival,
# MAGIC     -- distance_od,
# MAGIC     -- time_first_depart,
# MAGIC     -- time_last_arrival,
# MAGIC     train_local,
# MAGIC     train_class,
# MAGIC     train_priority,
# MAGIC     train_pso,
# MAGIC     train_perintis,
# MAGIC     sum(passenger) as total_passenger, 
# MAGIC     sum(amount) as total_amount
# MAGIC FROM
# MAGIC     dashboard_ticketing_aggregate_system_posko_raw
# MAGIC GROUP BY
# MAGIC     Date(period_first_depart),
# MAGIC     -- area_first_depart, 
# MAGIC     area_code_first_depart, 
# MAGIC     area_name_first_depart,
# MAGIC     train_number_merge,
# MAGIC     train_number,
# MAGIC     train_name,
# MAGIC     -- station_first_depart, 
# MAGIC     station_code_first_depart,
# MAGIC     station_code_last_arrival,
# MAGIC     -- distance_od,
# MAGIC     -- time_first_depart,
# MAGIC     -- time_last_arrival,
# MAGIC     train_local,
# MAGIC     train_class,
# MAGIC     train_priority,
# MAGIC     train_pso,
# MAGIC     train_perintis
# MAGIC     
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE aggregate_table AS
# MAGIC WITH 
# MAGIC     system as (
# MAGIC     SELECT
# MAGIC         Date(period_first_depart) as period_first_depart,
# MAGIC         -- area_first_depart, 
# MAGIC         area_code_first_depart, 
# MAGIC         area_name_first_depart,
# MAGIC         train_number_merge,
# MAGIC         train_number,
# MAGIC         train_name,
# MAGIC         -- station_first_depart, 
# MAGIC         station_code_first_depart,
# MAGIC         station_code_last_arrival,
# MAGIC         -- distance_od,
# MAGIC         -- time_first_depart,
# MAGIC         -- time_last_arrival,
# MAGIC         train_local,
# MAGIC         train_class,
# MAGIC         train_priority,
# MAGIC         train_pso,
# MAGIC         train_perintis,
# MAGIC         sum(passenger) as total_passenger, 
# MAGIC         sum(amount) as total_amount
# MAGIC     FROM
# MAGIC         dashboard_ticketing_aggregate_system_raw
# MAGIC     GROUP BY
# MAGIC         Date(period_first_depart),
# MAGIC         -- area_first_depart, 
# MAGIC         area_code_first_depart, 
# MAGIC         area_name_first_depart,
# MAGIC         train_number_merge,
# MAGIC         train_number,
# MAGIC         train_name,
# MAGIC         -- station_first_depart, 
# MAGIC         station_code_first_depart,
# MAGIC         station_code_last_arrival,
# MAGIC         -- distance_od,
# MAGIC         -- time_first_depart,
# MAGIC         -- time_last_arrival,
# MAGIC         train_local,
# MAGIC         train_class,
# MAGIC         train_priority,
# MAGIC         train_pso,
# MAGIC         train_perintis
# MAGIC     ),
# MAGIC     program as (
# MAGIC     SELECT
# MAGIC         if(Date(period_first_depart) is null, Date(period_last_arrival), Date(period_first_depart)) as period_first_depart,
# MAGIC         -- area_first_depart, 
# MAGIC         area_code_first_depart, 
# MAGIC         area_name_first_depart,
# MAGIC         train_number_merge,
# MAGIC         train_number,
# MAGIC         train_name,
# MAGIC         -- station_first_depart, 
# MAGIC         station_code_first_depart,
# MAGIC         station_code_last_arrival,
# MAGIC         -- distance_od,
# MAGIC         -- time_first_depart,
# MAGIC         -- time_last_arrival,
# MAGIC         train_local,
# MAGIC         train_class,
# MAGIC         train_priority,
# MAGIC         train_pso,
# MAGIC         train_perintis,
# MAGIC         sum(passenger) as total_passenger_program, 
# MAGIC         sum(amount) as total_amount_program
# MAGIC     FROM
# MAGIC         dashboard_ticketing_aggregate_program_raw
# MAGIC     GROUP BY
# MAGIC         if(Date(period_first_depart) is null, Date(period_last_arrival), Date(period_first_depart)),
# MAGIC         -- area_first_depart, 
# MAGIC         area_code_first_depart, 
# MAGIC         area_name_first_depart,
# MAGIC         train_number_merge,
# MAGIC         train_number,
# MAGIC         train_name,
# MAGIC         -- station_first_depart, 
# MAGIC         station_code_first_depart,
# MAGIC         station_code_last_arrival,
# MAGIC         -- distance_od,
# MAGIC         -- time_first_depart,
# MAGIC         -- time_last_arrival,
# MAGIC         train_local,
# MAGIC         train_class,
# MAGIC         train_priority,
# MAGIC         train_pso,
# MAGIC         train_perintis
# MAGIC     )
# MAGIC 
# MAGIC -- CREATE TABLE aggregate_table as
# MAGIC SELECT
# MAGIC     s.period_first_depart,
# MAGIC     -- area_first_depart, 
# MAGIC     s.area_code_first_depart, 
# MAGIC     s.area_name_first_depart,
# MAGIC     s.train_number_merge,
# MAGIC     s.train_number,
# MAGIC     s.train_name,
# MAGIC     -- station_first_depart, 
# MAGIC     s.station_code_first_depart,
# MAGIC     s.station_code_last_arrival,
# MAGIC     -- distance_od,
# MAGIC     -- time_first_depart,
# MAGIC     -- time_last_arrival,
# MAGIC     s.train_local,
# MAGIC     s.train_class,
# MAGIC     s.train_priority,
# MAGIC     s.train_pso,
# MAGIC     s.train_perintis,
# MAGIC     s.total_passenger, 
# MAGIC     s.total_amount,
# MAGIC     p.total_passenger_program,
# MAGIC     p.total_amount_program
# MAGIC FROM
# MAGIC     system as s
# MAGIC FULL OUTER JOIN
# MAGIC     program as p
# MAGIC ON 
# MAGIC     s.period_first_depart = p.period_first_depart   
# MAGIC     AND trim(upper(s.area_code_first_depart)) = trim(upper(p.area_code_first_depart))
# MAGIC     AND trim(upper(s.area_name_first_depart)) = trim(upper(p.area_name_first_depart))
# MAGIC     AND s.train_number_merge = p.train_number_merge
# MAGIC     AND s.train_number = p.train_number  
# MAGIC     AND trim(upper(s.train_name)) = trim(upper(p.train_name))
# MAGIC     AND trim(upper(s.station_code_first_depart)) = trim(upper(p.station_code_first_depart))
# MAGIC     AND trim(upper(s.station_code_last_arrival)) = trim(upper(p.station_code_last_arrival))
# MAGIC     AND s.train_local = p.train_local  
# MAGIC     AND trim(upper(s.train_class)) = trim(upper(p.train_class))
# MAGIC     AND s.train_priority = p.train_priority  
# MAGIC     AND s.train_pso = p.train_pso  
# MAGIC     AND s.train_perintis = p.train_perintis

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT sum(passenger), sum(amount) from dashboard_ticketing_aggregate_system_raw where period_first_depart between '2024-03-31' and '2024-04-21'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT sum(total_passenger_program), sum(total_amount_program) from aggregate_table2 where period_first_depart between '2024-03-31' and '2024-04-21'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT sum(passenger), sum(amount) from dashboard_ticketing_aggregate_program_raw where period_first_depart between '2024-03-31' and '2024-04-21'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select trim(' a')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select sum(total_passenger_program)
# MAGIC from(
# MAGIC     SELECT
# MAGIC         if(Date(period_first_depart) is null, Date(period_last_arrival), Date(period_first_depart)) as period_first_depart,
# MAGIC         -- area_first_depart, 
# MAGIC         area_code_first_depart, 
# MAGIC         area_name_first_depart,
# MAGIC         train_number_merge,
# MAGIC         train_number,
# MAGIC         train_name,
# MAGIC         -- station_first_depart, 
# MAGIC         station_code_first_depart,
# MAGIC         station_code_last_arrival,
# MAGIC         -- distance_od,
# MAGIC         -- time_first_depart,
# MAGIC         -- time_last_arrival,
# MAGIC         train_local,
# MAGIC         train_class,
# MAGIC         train_priority,
# MAGIC         train_pso,
# MAGIC         train_perintis,
# MAGIC         sum(passenger) as total_passenger_program, 
# MAGIC         sum(amount) as total_amount_program
# MAGIC     FROM
# MAGIC         dashboard_ticketing_aggregate_program_raw
# MAGIC     where period_first_depart between '2024-03-31' and '2024-04-21'
# MAGIC     GROUP BY
# MAGIC         if(Date(period_first_depart) is null, Date(period_last_arrival), Date(period_first_depart)),
# MAGIC         -- area_first_depart, 
# MAGIC         area_code_first_depart, 
# MAGIC         area_name_first_depart,
# MAGIC         train_number_merge,
# MAGIC         train_number,
# MAGIC         train_name,
# MAGIC         -- station_first_depart, 
# MAGIC         station_code_first_depart,
# MAGIC         station_code_last_arrival,
# MAGIC         -- distance_od,
# MAGIC         -- time_first_depart,
# MAGIC         -- time_last_arrival,
# MAGIC         train_local,
# MAGIC         train_class,
# MAGIC         train_priority,
# MAGIC         train_pso,
# MAGIC         train_perintis
# MAGIC     ) as s
# MAGIC left anti join
# MAGIC aggregate_table2 as p
# MAGIC on
# MAGIC     s.period_first_depart = p.period_first_depart   
# MAGIC     AND trim(upper(s.area_code_first_depart)) = trim(upper(p.area_code_first_depart))
# MAGIC     AND trim(upper(s.area_name_first_depart)) = trim(upper(p.area_name_first_depart))
# MAGIC     AND s.train_number_merge = p.train_number_merge
# MAGIC     AND s.train_number = p.train_number  
# MAGIC     AND trim(upper(s.train_name)) = trim(upper(p.train_name))
# MAGIC     AND trim(upper(s.station_code_first_depart)) = trim(upper(p.station_code_first_depart))
# MAGIC     AND trim(upper(s.station_code_last_arrival)) = trim(upper(p.station_code_last_arrival))
# MAGIC     AND s.train_local = p.train_local  
# MAGIC     AND trim(upper(s.train_class)) = trim(upper(p.train_class))
# MAGIC     AND s.train_priority = p.train_priority  
# MAGIC     AND s.train_pso = p.train_pso  
# MAGIC     AND s.train_perintis = p.train_perintis

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from aggregate_table2 where period_first_depart = '2024-04-07' and  area_code_first_depart = 'Daop 2' 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Step 1: Create the Posko Event Table with the 'Type' column after 'Posko'
# MAGIC CREATE TABLE PoskoEventTable (
# MAGIC     Posko VARCHAR(100),  -- Column for the event name (Lebaran 2019, 2020, etc.)
# MAGIC     Type VARCHAR(50),    -- New column to store the event type, in this case, 'Lebaran'
# MAGIC     StartDate DATE,      -- Start date of the event
# MAGIC     EndDate DATE         -- End date of the event
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC -- Step 2: Insert the event date ranges into the table with the 'Type' column
# MAGIC INSERT INTO PoskoEventTable (Posko, Type, StartDate, EndDate)
# MAGIC VALUES
# MAGIC     ('Lebaran 2019', 'Lebaran', '2019-05-26', '2019-06-16'),
# MAGIC     ('Lebaran 2020', 'Lebaran', '2020-05-24', '2020-06-14'),
# MAGIC     ('Lebaran 2021', 'Lebaran', '2021-05-12', '2021-06-02'),
# MAGIC     ('Lebaran 2022', 'Lebaran', '2022-04-22', '2022-05-13'),
# MAGIC     ('Lebaran 2023', 'Lebaran', '2023-04-12', '2023-05-03'),
# MAGIC     ('Lebaran 2024', 'Lebaran', '2024-03-31', '2024-04-21'),
# MAGIC     ('Lebaran 2025', 'Lebaran', '2025-03-21', '2025-04-11');
# MAGIC 
# MAGIC -- Step 3: Verify the data
# MAGIC SELECT * FROM PoskoEventTable;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Step 1: Create the Date Table
# MAGIC CREATE OR REPLACE TABLE DateTable (
# MAGIC     Date DATE,
# MAGIC     Year INT,
# MAGIC     Month INT,
# MAGIC     Day INT,
# MAGIC     Weekday INT,
# MAGIC     Quarter INT,
# MAGIC     DayOfYear INT,
# MAGIC     IsWeekend BOOLEAN
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC -- Step 2: Generate the Date Sequence and Populate the DateTable
# MAGIC WITH RECURSIVE DateSequence AS (
# MAGIC     -- Starting point (first date in the sequence)
# MAGIC     SELECT DATE '2019-01-01' AS Date
# MAGIC     UNION ALL
# MAGIC     -- Recursive part: adding one day at a time
# MAGIC     SELECT DATE_ADD(Date, INTERVAL 1 DAY)
# MAGIC     FROM DateSequence
# MAGIC     WHERE Date < '2025-12-31'
# MAGIC )
# MAGIC -- Insert data into DateTable
# MAGIC INSERT INTO DateTable (Date, Year, Month, Day, Weekday, Quarter, DayOfYear, IsWeekend)
# MAGIC SELECT 
# MAGIC     Date,
# MAGIC     YEAR(Date) AS Year,
# MAGIC     MONTH(Date) AS Month,
# MAGIC     DAY(Date) AS Day,
# MAGIC     DAYOFWEEK(Date) AS Weekday,
# MAGIC     QUARTER(Date) AS Quarter,
# MAGIC     DAYOFYEAR(Date) AS DayOfYear,
# MAGIC     CASE WHEN DAYOFWEEK(Date) IN (1, 7) THEN TRUE ELSE FALSE END AS IsWeekend
# MAGIC FROM DateSequence;
# MAGIC 
# MAGIC -- Step 3: Verify the data
# MAGIC SELECT * FROM DateTable LIMIT 10;  -- Limit to the first 10 rows for verification
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE aggregate_system AS
# MAGIC     SELECT
# MAGIC         Date(period_first_depart) as period_first_depart,
# MAGIC         -- area_first_depart, 
# MAGIC         area_code_first_depart, 
# MAGIC         area_name_first_depart,
# MAGIC         train_number_merge,
# MAGIC         train_number,
# MAGIC         train_name,
# MAGIC         -- station_first_depart, 
# MAGIC         station_code_first_depart,
# MAGIC         station_code_last_arrival,
# MAGIC         -- distance_od,
# MAGIC         -- time_first_depart,
# MAGIC         -- time_last_arrival,
# MAGIC         train_local,
# MAGIC         train_class,
# MAGIC         train_priority,
# MAGIC         train_pso,
# MAGIC         train_perintis,
# MAGIC         sum(passenger) as total_passenger, 
# MAGIC         sum(amount) as total_amount
# MAGIC     FROM
# MAGIC         dashboard_ticketing_aggregate_system_raw
# MAGIC     GROUP BY
# MAGIC         Date(period_first_depart),
# MAGIC         -- area_first_depart, 
# MAGIC         area_code_first_depart, 
# MAGIC         area_name_first_depart,
# MAGIC         train_number_merge,
# MAGIC         train_number,
# MAGIC         train_name,
# MAGIC         -- station_first_depart, 
# MAGIC         station_code_first_depart,
# MAGIC         station_code_last_arrival,
# MAGIC         -- distance_od,
# MAGIC         -- time_first_depart,
# MAGIC         -- time_last_arrival,
# MAGIC         train_local,
# MAGIC         train_class,
# MAGIC         train_priority,
# MAGIC         train_pso,
# MAGIC         train_perintis
# MAGIC     

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE aggregate_program AS
# MAGIC SELECT
# MAGIC         if(Date(period_first_depart) is null, Date(period_last_arrival), Date(period_first_depart)) as period_first_depart,
# MAGIC         -- area_first_depart, 
# MAGIC         area_code_first_depart, 
# MAGIC         area_name_first_depart,
# MAGIC         train_number_merge,
# MAGIC         train_number,
# MAGIC         train_name,
# MAGIC         -- station_first_depart, 
# MAGIC         station_code_first_depart,
# MAGIC         station_code_last_arrival,
# MAGIC         -- distance_od,
# MAGIC         -- time_first_depart,
# MAGIC         -- time_last_arrival,
# MAGIC         train_local,
# MAGIC         train_class,
# MAGIC         train_priority,
# MAGIC         train_pso,
# MAGIC         train_perintis,
# MAGIC         sum(passenger) as total_passenger_program, 
# MAGIC         sum(amount) as total_amount_program
# MAGIC     FROM
# MAGIC         dashboard_ticketing_aggregate_program_raw
# MAGIC     GROUP BY
# MAGIC         if(Date(period_first_depart) is null, Date(period_last_arrival), Date(period_first_depart)),
# MAGIC         -- area_first_depart, 
# MAGIC         area_code_first_depart, 
# MAGIC         area_name_first_depart,
# MAGIC         train_number_merge,
# MAGIC         train_number,
# MAGIC         train_name,
# MAGIC         -- station_first_depart, 
# MAGIC         station_code_first_depart,
# MAGIC         station_code_last_arrival,
# MAGIC         -- distance_od,
# MAGIC         -- time_first_depart,
# MAGIC         -- time_last_arrival,
# MAGIC         train_local,
# MAGIC         train_class,
# MAGIC         train_priority,
# MAGIC         train_pso,
# MAGIC         train_perintis

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(*), c_stasiun_code from dashboard_ticketing_stasiun_raw group by c_stasiun_code

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * from dashboard_ticketing_aggregate_program_raw where station_first_depart = 828

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- create or replace table dashboard_ticketing_train_raw
# MAGIC with a as(
# MAGIC     select * from aggregate_system
# MAGIC     union all
# MAGIC     select * from aggregate_program
# MAGIC )
# MAGIC 
# MAGIC 
# MAGIC Select distinct concat(concat_ws(' (', train_name, train_number),')') as train_id, train_number train_name from a

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC create or replace table dashboard_ticketing_train_raw
# MAGIC with data as (
# MAGIC     select 
# MAGIC         train_number, train_name,  period_first_depart,
# MAGIC         train_priority, train_local, 
# MAGIC         train_perintis
# MAGIC     from aggregate_system
# MAGIC     union all
# MAGIC     select
# MAGIC         train_number, train_name,  period_first_depart,
# MAGIC         train_priority, train_local, 
# MAGIC         train_perintis
# MAGIC     from aggregate_program
# MAGIC )
# MAGIC 
# MAGIC select 
# MAGIC     -- count(
# MAGIC     distinct 
# MAGIC     concat(concat_ws(' (', train_name, train_number),')') as train_id, 
# MAGIC     train_number, 
# MAGIC     -- train_number_merge,
# MAGIC     train_name
# MAGIC     -- ,data.area_code_first_depart
# MAGIC     -- ,data.area_code_last_arrival
# MAGIC     -- ,data.train_class
# MAGIC     ,data.train_priority
# MAGIC     -- ,data.train_pso
# MAGIC     ,data.train_local
# MAGIC     -- ,data.station_code_first_depart
# MAGIC     -- ,data.station_code_last_arrival
# MAGIC     ,data.train_perintis
# MAGIC     , year(period_first_depart) as year
# MAGIC     -- )
# MAGIC from 
# MAGIC     data
# MAGIC -- UNION ALL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC ALTER TABLE dashboard_ticketing_train_raw
# MAGIC ADD COLUMN
# MAGIC     train_local_name varchar(15),
# MAGIC     train_priority_name varchar(15),
# MAGIC     train_perintis_name varchar(15);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC UPDATE dashboard_ticketing_train_raw
# MAGIC SET train_local_name = CASE
# MAGIC                 WHEN train_local = 1 THEN 'KA Lokal'
# MAGIC                 WHEN train_local = 0 THEN 'KA Utama'
# MAGIC                 ELSE train_local -- Retain the original value if no condition is met
# MAGIC             END
# MAGIC -- WHERE train_local IN ('EKS', 'EKO', 'BIS');

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC UPDATE dashboard_ticketing_train_raw
# MAGIC SET train_priority_name = CASE
# MAGIC                 WHEN train_priority = 1 THEN 'KA Priority'
# MAGIC                 WHEN train_priority = 0 THEN 'Non Priority'
# MAGIC                 ELSE train_priority -- Retain the original value if no condition is met
# MAGIC             END

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC UPDATE dashboard_ticketing_train_raw
# MAGIC SET train_perintis_name = CASE
# MAGIC                 WHEN train_perintis = 1 THEN 'Perintis'
# MAGIC                 WHEN train_perintis = 0 THEN 'Non Perintis'
# MAGIC                 ELSE train_perintis -- Retain the original value if no condition is met
# MAGIC             END

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC ALTER TABLE dashboard_ticketing_train_class_raw
# MAGIC ADD COLUMN
# MAGIC     class_name varchar(15)
# MAGIC     -- train_priority_name varchar(15),
# MAGIC     -- train_perintis_name varchar(15);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC UPDATE dashboard_ticketing_train_class_raw
# MAGIC SET class_name = CASE
# MAGIC                 WHEN class = 'EKS' THEN 'Eksekutif'
# MAGIC                 WHEN class = 'EKO' THEN 'Ekonomi'
# MAGIC                 WHEN class = 'BIS' THEN 'Bisnis'
# MAGIC                 ELSE class -- Retain the original value if no condition is met
# MAGIC             END

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC ALTER TABLE dashboard_ticketing_train_pso_raw
# MAGIC ADD COLUMN
# MAGIC     pso_name varchar(15)
# MAGIC     -- train_priority_name varchar(15),
# MAGIC     -- train_perintis_name varchar(15);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC UPDATE dashboard_ticketing_train_pso_raw
# MAGIC SET pso_name = CASE
# MAGIC                 WHEN pso > 0 THEN 'PSO'
# MAGIC                 WHEN pso = 0 THEN 'Non PSO'
# MAGIC                 ELSE pso -- Retain the original value if no condition is met
# MAGIC             END

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC ALTER TABLE dashboard_ticketing_area_raw
# MAGIC ADD COLUMN
# MAGIC     Wilayah STRING

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC SELECT 
# MAGIC     case
# MAGIC         WHEN c_area_name like 'DAOP%' then 'Jawa'
# MAGIC         WHEN c_area_name like 'DIV%' then 'Sumatra'
# MAGIC         ELSE NULL
# MAGIC     END
# MAGIC from dashboard_ticketing_area_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC UPDATE dashboard_ticketing_area_raw
# MAGIC SET Wilayah = CASE
# MAGIC         WHEN c_area_name like 'DAOP%' then 'Jawa'
# MAGIC         WHEN c_area_name like 'DIV%' then 'Sumatra'
# MAGIC         ELSE NULL
# MAGIC     END

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT count(*), count(distinct train_id) from dashboard_ticketing_train_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select 
# MAGIC     train_number, 
# MAGIC     train_name,
# MAGIC     year(period_first_depart),
# MAGIC     COUNT(distinct aggregate_system.train_number_merge) a,
# MAGIC     COUNT(distinct aggregate_system.area_code_first_depart) b,
# MAGIC     -- COUNT(distinct aggregate_system.area_code_last_arrival) c,
# MAGIC     COUNT(distinct aggregate_system.train_class) d,
# MAGIC     COUNT(distinct aggregate_system.train_priority) e,
# MAGIC     COUNT(distinct aggregate_system.train_pso) f,
# MAGIC     COUNT(distinct aggregate_system.train_local) g,
# MAGIC     COUNT(distinct aggregate_system.station_code_first_depart) h,
# MAGIC     COUNT(distinct aggregate_system.station_code_last_arrival) i,
# MAGIC     COUNT(distinct aggregate_system.train_perintis) j
# MAGIC from aggregate_system 
# MAGIC group by 
# MAGIC     train_number, 
# MAGIC     train_name,
# MAGIC     year(period_first_depart)
# MAGIC having f > 1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC with data as (
# MAGIC     select 
# MAGIC         train_number, train_name,  period_first_depart,
# MAGIC         train_priority, train_local, 
# MAGIC         train_perintis
# MAGIC     from aggregate_system
# MAGIC     union all
# MAGIC     select
# MAGIC         train_number, train_name,  period_first_depart,
# MAGIC         train_priority, train_local, 
# MAGIC         train_perintis
# MAGIC     from aggregate_program
# MAGIC )
# MAGIC 
# MAGIC select 
# MAGIC     train_number, 
# MAGIC     train_name,
# MAGIC     year(period_first_depart),
# MAGIC     -- COUNT(distinct data.train_number_merge) a,
# MAGIC     -- COUNT(distinct data.area_code_first_depart) b,
# MAGIC     -- COUNT(distinct data.area_code_last_arrival) c,
# MAGIC     -- COUNT(distinct data.train_class) d,
# MAGIC     COUNT(distinct data.train_priority) e,
# MAGIC     -- COUNT(distinct data.train_pso) f,
# MAGIC     COUNT(distinct data.train_local) g,
# MAGIC     -- COUNT(distinct data.station_code_first_depart) h,
# MAGIC     -- COUNT(distinct data.station_code_last_arrival) i,
# MAGIC     COUNT(distinct data.train_perintis) j
# MAGIC from data 
# MAGIC group by 
# MAGIC     train_number, 
# MAGIC     train_name,
# MAGIC     year(period_first_depart)
# MAGIC -- having j > 1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- create or replace table dashboard_ticketing_train_raw
# MAGIC with a as(
# MAGIC     select * from aggregate_system
# MAGIC     union all
# MAGIC     select * from aggregate_program
# MAGIC )
# MAGIC 
# MAGIC Select count(distinct concat(concat_ws(' (', train_name, train_number),')')) as train_id
# MAGIC -- , train_number_merge, train_name 
# MAGIC from a
# MAGIC where train_name is not null and train_number is not null

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM dashboard_ticketing_train_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT count(*), count( distinct concat_ws(' ' ,train_name, train_number, train_number_merge)) from dashboard_ticketing_train_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- DELETE from dashboard_ticketing_area_raw where c_area_id = 9

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT count(*), count(distinct id) from dashboard_ticketing_aggregate_program_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * from years_with_prev

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO numbers (num) VALUES
# MAGIC (0), (1), (2), (3), (4), (5), (6), (7), (8), (9),
# MAGIC (10), (11), (12), (13), (14), (15);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE years as
# MAGIC (
# MAGIC WITH year_range AS (
# MAGIC     SELECT 2015 + num AS year
# MAGIC     FROM numbers
# MAGIC     WHERE 2015 + num <= 2030  -- Adjust based on your desired year range
# MAGIC ),
# MAGIC prev_years AS (
# MAGIC     SELECT year AS prev_year
# MAGIC     FROM year_range
# MAGIC     UNION ALL
# MAGIC     SELECT year - 1
# MAGIC     FROM year_range
# MAGIC     UNION ALL
# MAGIC     SELECT year - 2
# MAGIC     FROM year_range
# MAGIC )
# MAGIC SELECT distinct year, prev_year
# MAGIC FROM year_range
# MAGIC JOIN prev_years ON prev_years.prev_year BETWEEN year - 2 AND year
# MAGIC ORDER BY year, prev_year
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT SUM(passenger) from dashboard_ticketing_aggregate_program_raw where period_first_depart between '2024-01-01' and '2024-12-31'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT min(period_first_depart) from dashboard_ticketing_aggregate_system_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

year = range(2015, 2031) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from itertools import product

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

y = list(product(year, year))
x = list(map(lambda x: x +("Realisasi "+str(x[1]),), y))
x

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows  = []
type = ['Penumpang (Orang)', 'Pencapaian Penumpang Terhadap (%)', 'Pendapatan', 'Pencapaian Pendapatan Terhadap (%)']
for i in range(1, 5):
    rows += list((i, type[i-1])+ x for x in list(filter(lambda x: (x[0] - x[1]) <= 2 and x[0] >= x[1] , x)) + list(map(lambda x: x + ("Program " + str(x[1]),),list(zip(range(2015, 2031), range(2015, 2031))))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pd.DataFrame(rows, columns=["id", "category", "year", "prev_year", "label"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

datayear = spark.createDataFrame(pd.DataFrame(rows, columns=["id", "category", "year", "prev_year", "label"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

datayear

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

datayear.write.format("delta").mode('overwrite').saveAsTable('year_table')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT sum(total_amount) from aggregate_system WHERE period_first_depart between '2025-03-21' and '2025-04-11';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT sum(passenger) from dashboard_ticketing_aggregate_system_incr WHERE period_first_depart between '2025-03-21' and '2025-04-11';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select distinct period_first_depart from dashboard_ticketing_aggregate_program_raw where period_first_depart between '2023-01-01' and '2023-12-31'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select distinct period_last_arrival from dashboard_ticketing_aggregate_program_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from aggregate_system where train_number_merge = "3L"  and period_first_depart > '2025-01-01'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- DELETE from aggregate_system where period_first_depart > '2025-01-01' and period_first_depart not between '2025-03-21' and '2025-04-11'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- DELETE from dashboard_ticketing_aggregate_system_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select sum(passenger) from dashboard_ticketing_aggregate_system_raw where period_last_arrival between '2024-03-31' and '2024-04-21' and id is not null and company in (0,2)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select count(distinct id), count(distinct nomor_ka, nama_ka) from dashboard_ticketing_biop_revenue_raw limit 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(*) from dashboard_ticketing_train_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select distinct company from dashboard_ticketing_aggregate_system_raw where period_first_depart > '2025-01-01'


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from dashboard_ticketing_aggregate_system_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select sum(passenger) from dashboard_ticketing_aggregate_system_raw where period_first_depart between '2024-03-31' and '2024-04-21' and company in (0,2)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select sum(total_passenger) from aggregate_system where period_first_depart between '2024-01-01' and '2024-03-30'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- SELECT distinct area_code_first_depart, area_name_first_depart from dashboard_ticketing_aggregate_system_raw
# MAGIC SELECT distinct c_area_alias, c_area_name from dashboard_ticketing_area_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT sum(passenger) as volume, sum(amount) pendapatan, max(last_update) as max_update, min(last_update) as min_update from dashboard_ticketing_aggregate_system_raw where id is not null and period_first_depart between '2024-03-31' and '2024-04-21' and not(passenger = 0 and amount = 0) and company in (0,2);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM dashboard_ticketing_biop_revenue_raw where tanggal_ka between '2025-03-21' and '2025-04-11'
# MAGIC 
# MAGIC realisasi (aggregate_system) / kapasitas (biop_revenue)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT max(c_stasiun_id), count(*) from dashboard_ticketing_stasiun_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 
# MAGIC     train_number,
# MAGIC     -- train_number_merge,
# MAGIC     train_name,
# MAGIC     COUNT(distinct train_number_merge) a,
# MAGIC     COUNT(distinct area_code_first_depart) b,
# MAGIC     COUNT(distinct area_code_last_arrival) c,
# MAGIC     COUNT(distinct train_class) d,
# MAGIC     COUNT(distinct train_priority) e,
# MAGIC     COUNT(distinct train_pso) f,
# MAGIC     COUNT(distinct train_local) g,
# MAGIC     COUNT(distinct station_first_depart) h,
# MAGIC     COUNT(distinct station_last_arrival) i,
# MAGIC     COUNT(distinct train_perintis) j
# MAGIC from 
# MAGIC     dashboard_ticketing_aggregate_system_raw
# MAGIC -- where
# MAGIC     -- id is not null 
# MAGIC     -- and period_first_depart between '2021-04-10' and '2021-06-01' 
# MAGIC     -- and not(passenger = 0 and amount = 0) 
# MAGIC     -- and company in (0,2)
# MAGIC     -- and train_priority = 0
# MAGIC     -- and source_data = '1'
# MAGIC     -- and train_number != train_number_merge
# MAGIC GROUP BY
# MAGIC     train_number,
# MAGIC     -- train_number_merge,
# MAGIC     train_name

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select distinct train_number, train_name, station_first_depart, station_last_arrival from dashboard_ticketing_aggregate_system_raw --where train_number in ('552 - 557')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE dashboard_ticketing_train_class_raw (
# MAGIC     class VARCHAR(10)
# MAGIC );
# MAGIC 
# MAGIC -- Insert values into the train_class table
# MAGIC INSERT INTO dashboard_ticketing_train_class_raw (class)
# MAGIC VALUES ('EKS'), ('EKO'), ('BIS');

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select distinct train_pso from aggregate_system

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE dashboard_ticketing_train_pso_raw (
# MAGIC     pso int
# MAGIC );
# MAGIC 
# MAGIC -- Insert values into the train_pso table
# MAGIC INSERT INTO dashboard_ticketing_train_pso_raw (pso)
# MAGIC VALUES (0), (1), (2), (3), (4), (5);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT sum(amount), sum(passenger) from dashboard_ticketing_aggregate_system_raw where source_data  ='1' and period_first_depart between '2024-01-01' and '2024-06-30' and company in (0,2)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT sum(amount), sum(passenger), max(last_update) from dashboard_ticketing_aggregate_system_incr where source_data  ='1' and period_first_depart between '2025-01-01' and '2025-06-30' and company in (0,2) and train_priority =0

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT max(train_priority) from aggregate_system where period_first_depart between '2025-03-21' and '2025-04-11' and feeder =1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT trim('  a')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
