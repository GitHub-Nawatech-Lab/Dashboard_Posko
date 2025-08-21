# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "92757ff8-e232-4a85-a0df-0b4ff8fbc1f5",
# META       "default_lakehouse_name": "data",
# META       "default_lakehouse_workspace_id": "371caf18-1651-4320-b03d-ddd574e85e48",
# META       "known_lakehouses": [
# META         {
# META           "id": "92757ff8-e232-4a85-a0df-0b4ff8fbc1f5"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# 
# #### Run the cell below to install the required packages for Copilot


# CELL ********************


#Run this cell to install the required packages for Copilot
%load_ext dscopilot_installer
%activate_dscopilot


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT count(*), max(period_last_arrival), max(period_first_depart), max(last_update) FROM dashboard_ticketing_aggregate_system_posko_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM data.dashboard_ticketing_aggregate_system_posko_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.dropDuplicates(df.columns[1:]).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 
# MAGIC     /* tambahan field ----------------------------- */ 
# MAGIC     -- Extract(year FROM Date(period_first_depart)) AS year, 
# MAGIC     year(period_first_depart),
# MAGIC     /* default field ----------------------------- */ 
# MAGIC     Date(period_first_depart) AS date_first_depart,
# MAGIC     train_number_merge,
# MAGIC     train_number,
# MAGIC     train_name,
# MAGIC     station_code_first_depart,
# MAGIC     station_code_last_arrival,
# MAGIC     distance_od,
# MAGIC     time_first_depart,
# MAGIC     time_last_arrival,
# MAGIC     train_local,
# MAGIC     train_class,
# MAGIC     train_priority,
# MAGIC     train_pso,
# MAGIC     train_perintis
# MAGIC     -- /* sum ----------------------------- */
# MAGIC     -- Sum(passenger) AS passenger ,
# MAGIC     -- Sum(amount) AS amount,
# MAGIC     -- Sum(distance) AS distance
# MAGIC FROM 
# MAGIC     dashboard_ticketing_aggregate_system_posko_raw
# MAGIC WHERE 
# MAGIC     id IS NOT NULL
# MAGIC     AND Date(period_first_depart) BETWEEN '2023-04-12' AND '2023-05-03'
# MAGIC -- LIMIT 10
# MAGIC GROUP BY 
# MAGIC     /* tambahan field ----------------------------- */ 
# MAGIC     year(period_first_depart),
# MAGIC     /* default field ----------------------------- */ 
# MAGIC     Date(period_first_depart),
# MAGIC     train_number_merge ,
# MAGIC     train_number,
# MAGIC     train_name,
# MAGIC     station_code_first_depart,
# MAGIC     station_code_last_arrival,
# MAGIC     distance_od,
# MAGIC     time_first_depart,
# MAGIC     time_last_arrival,
# MAGIC     train_local,
# MAGIC     train_class,
# MAGIC     train_priority,
# MAGIC     train_pso,
# MAGIC     train_perintis

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 
# MAGIC     year,
# MAGIC     Sum(train_count) AS train_count,
# MAGIC     Sum(passenger) AS passenger,
# MAGIC     Sum(amount) AS amount,
# MAGIC     Sum(distance) AS distance,
# MAGIC     Sum(capacity) AS capacity,
# MAGIC     Sum(capacity_stamform) AS capacity_stamform,
# MAGIC     Sum(seat) AS seat,
# MAGIC     Sum(biop) AS biop,
# MAGIC     Sum(fixed_cost) AS fixed_cost,
# MAGIC     Sum(variable_cost) AS variable_cost
# MAGIC FROM
# MAGIC     (
# MAGIC         SELECT 
# MAGIC             NULL AS year,
# MAGIC             0 AS train_count,
# MAGIC             0 AS passenger,
# MAGIC             0 AS amount,
# MAGIC             0 AS distance,
# MAGIC             0 AS capacity,
# MAGIC             0 AS capacity_stamform,
# MAGIC             0 AS seat,
# MAGIC             0 AS biop,
# MAGIC             0 AS fixed_cost,
# MAGIC             0 AS variable_cost
# MAGIC         FROM 
# MAGIC             dashboard_ticketing_aggregate_system_posko_raw
# MAGIC         WHERE 
# MAGIC             id IS NULL
# MAGIC         UNION ALL
# MAGIC         SELECT 
# MAGIC             year,
# MAGIC             0 AS train_count,
# MAGIC             Sum(passenger) AS passenger,
# MAGIC             Sum(amount) AS amount,
# MAGIC             Sum(distance) AS distance,
# MAGIC             Sum(capacity) AS capacity,
# MAGIC             Sum(capacity_stamform) AS capacity_stamform,
# MAGIC             Sum(seat) AS seat,
# MAGIC             0 AS biop,
# MAGIC             0 AS fixed_cost,
# MAGIC             0 AS variable_cost
# MAGIC         FROM
# MAGIC              (
# MAGIC                 SELECT 
# MAGIC                 /* tambahan field ----------------------------- */ 
# MAGIC                     aggregate.year, 
# MAGIC                     /* default field group join biop ----------------------------- */ 
# MAGIC                     aggregate.date_first_depart,
# MAGIC                     aggregate.train_number_merge,
# MAGIC                     aggregate.train_number,
# MAGIC                     aggregate.train_name,
# MAGIC                     aggregate.train_class,
# MAGIC                     /* sum ----------------------------- */ 
# MAGIC                     Sum(passenger) AS passenger,
# MAGIC                     Sum(amount) AS amount,
# MAGIC                     0 AS distance,
# MAGIC                     0 AS capacity,
# MAGIC                     0 AS capacity_stamform,
# MAGIC                     0 AS seat
# MAGIC                 FROM
# MAGIC                     (
# MAGIC                         SELECT 
# MAGIC                             /* tambahan field ----------------------------- */ 
# MAGIC                             YEAR(period_first_depart) as year,
# MAGIC                             /* default field ----------------------------- */ 
# MAGIC                             Date(period_first_depart) AS date_first_depart,
# MAGIC                             train_number_merge,
# MAGIC                             train_number,
# MAGIC                             train_name,
# MAGIC                             station_code_first_depart,
# MAGIC                             station_code_last_arrival,
# MAGIC                             distance_od,
# MAGIC                             time_first_depart,
# MAGIC                             time_last_arrival,
# MAGIC                             train_local,
# MAGIC                             train_class,
# MAGIC                             train_priority,
# MAGIC                             train_pso,
# MAGIC                             train_perintis,/* sum ----------------------------- */ 
# MAGIC                             Sum(passenger) AS passenger ,
# MAGIC                             Sum(amount) AS amount,
# MAGIC                             Sum(distance) AS distance
# MAGIC                         FROM 
# MAGIC                             dashboard_ticketing_aggregate_system_posko_raw
# MAGIC                         WHERE 
# MAGIC                             id IS NOT NULL
# MAGIC                             AND Date(period_first_depart) BETWEEN '2023-04-12' AND '2023-05-03'
# MAGIC                         GROUP BY 
# MAGIC                             /* tambahan field ----------------------------- */ 
# MAGIC                             YEAR(period_first_depart),
# MAGIC                             /* default field ----------------------------- */ 
# MAGIC                             Date(period_first_depart),
# MAGIC                             train_number_merge ,
# MAGIC                             train_number,
# MAGIC                             train_name,
# MAGIC                             station_code_first_depart,
# MAGIC                             station_code_last_arrival,
# MAGIC                             distance_od,
# MAGIC                             time_first_depart,
# MAGIC                             time_last_arrival,
# MAGIC                             train_local,
# MAGIC                             train_class,
# MAGIC                             train_priority,
# MAGIC                             train_pso,
# MAGIC                             train_perintis
# MAGIC                 ) aggregate 
# MAGIC             GROUP BY /* tambahan field ----------------------------- */ 
# MAGIC                 aggregate.year, /* default field group join biop ----------------------------- */ aggregate.date_first_depart,
# MAGIC                 aggregate.train_number_merge,
# MAGIC                 aggregate.train_number,
# MAGIC                 aggregate.train_name,
# MAGIC                 aggregate.train_class
# MAGIC         ) data GROUP BY year
# MAGIC     ) program GROUP BY year
# MAGIC ORDER BY year

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql  
# MAGIC 
# MAGIC SELECT 
# MAGIC             year,
# MAGIC             0 AS train_count,
# MAGIC             Sum(passenger) AS passenger,
# MAGIC             Sum(amount) AS amount,
# MAGIC             Sum(distance) AS distance,
# MAGIC             Sum(capacity) AS capacity,
# MAGIC             Sum(capacity_stamform) AS capacity_stamform,
# MAGIC             Sum(seat) AS seat,
# MAGIC             0 AS biop,
# MAGIC             0 AS fixed_cost,
# MAGIC             0 AS variable_cost
# MAGIC         FROM
# MAGIC              (
# MAGIC                 SELECT 
# MAGIC                 /* tambahan field ----------------------------- */ 
# MAGIC                     aggregate.year, 
# MAGIC                     /* default field group join biop ----------------------------- */ 
# MAGIC                     aggregate.date_first_depart,
# MAGIC                     aggregate.train_number_merge,
# MAGIC                     aggregate.train_number,
# MAGIC                     aggregate.train_name,
# MAGIC                     aggregate.train_class,
# MAGIC                     /* sum ----------------------------- */ 
# MAGIC                     Sum(passenger) AS passenger,
# MAGIC                     Sum(amount) AS amount,
# MAGIC                     0 AS distance,
# MAGIC                     0 AS capacity,
# MAGIC                     0 AS capacity_stamform,
# MAGIC                     0 AS seat
# MAGIC                 FROM
# MAGIC                     (
# MAGIC                         SELECT 
# MAGIC                             /* tambahan field ----------------------------- */ 
# MAGIC                             -- Extract(year FROM Date(period_first_depart)) AS year,
# MAGIC                             YEAR(period_first_depart) as year,
# MAGIC                             /* default field ----------------------------- */ 
# MAGIC                             Date(period_first_depart) AS date_first_depart,
# MAGIC                             train_number_merge,
# MAGIC                             train_number,
# MAGIC                             train_name,
# MAGIC                             station_code_first_depart,
# MAGIC                             station_code_last_arrival,
# MAGIC                             distance_od,
# MAGIC                             time_first_depart,
# MAGIC                             time_last_arrival,
# MAGIC                             train_local,
# MAGIC                             train_class,
# MAGIC                             train_priority,
# MAGIC                             train_pso,
# MAGIC                             train_perintis,/* sum ----------------------------- */ 
# MAGIC                             Sum(passenger) AS passenger ,
# MAGIC                             Sum(amount) AS amount,
# MAGIC                             Sum(distance) AS distance
# MAGIC                         FROM 
# MAGIC                             dashboard_ticketing_aggregate_system_posko_raw
# MAGIC                         WHERE 
# MAGIC                             id IS NOT NULL
# MAGIC                             AND Date(period_first_depart) BETWEEN '2023-04-12' AND '2023-05-03'
# MAGIC                         GROUP BY 
# MAGIC                             /* tambahan field ----------------------------- */ 
# MAGIC                             -- Extract(year FROM Date(period_first_depart)), 
# MAGIC                             year(period_first_depart),
# MAGIC                             /* default field ----------------------------- */ 
# MAGIC                             Date(period_first_depart),
# MAGIC                             train_number_merge ,
# MAGIC                             train_number,
# MAGIC                             train_name,
# MAGIC                             station_code_first_depart,
# MAGIC                             station_code_last_arrival,
# MAGIC                             distance_od,
# MAGIC                             time_first_depart,
# MAGIC                             time_last_arrival,
# MAGIC                             train_local,
# MAGIC                             train_class,
# MAGIC                             train_priority,
# MAGIC                             train_pso,
# MAGIC                             train_perintis
# MAGIC                 ) aggregate 
# MAGIC             GROUP BY /* tambahan field ----------------------------- */ 
# MAGIC                 aggregate.year, /* default field group join biop ----------------------------- */ aggregate.date_first_depart,
# MAGIC                 aggregate.train_number_merge,
# MAGIC                 aggregate.train_number,
# MAGIC                 aggregate.train_name,
# MAGIC                 aggregate.train_class
# MAGIC         ) data GROUP BY year

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 
# MAGIC                 /* tambahan field ----------------------------- */ 
# MAGIC                     aggregate.year, 
# MAGIC                     /* default field group join biop ----------------------------- */ 
# MAGIC                     aggregate.date_first_depart,
# MAGIC                     aggregate.train_number_merge,
# MAGIC                     aggregate.train_number,
# MAGIC                     aggregate.train_name,
# MAGIC                     aggregate.train_class,
# MAGIC                     /* sum ----------------------------- */ 
# MAGIC                     Sum(passenger) AS passenger,
# MAGIC                     Sum(amount) AS amount,
# MAGIC                     0 AS distance,
# MAGIC                     0 AS capacity,
# MAGIC                     0 AS capacity_stamform,
# MAGIC                     0 AS seat
# MAGIC                 FROM
# MAGIC                     (
# MAGIC                         SELECT 
# MAGIC                             /* tambahan field ----------------------------- */ 
# MAGIC                             -- Extract(year FROM Date(period_first_depart)) AS year,
# MAGIC                             YEAR(period_first_depart) as year,
# MAGIC                             /* default field ----------------------------- */ 
# MAGIC                             Date(period_first_depart) AS date_first_depart,
# MAGIC                             train_number_merge,
# MAGIC                             train_number,
# MAGIC                             train_name,
# MAGIC                             station_code_first_depart,
# MAGIC                             station_code_last_arrival,
# MAGIC                             distance_od,
# MAGIC                             time_first_depart,
# MAGIC                             time_last_arrival,
# MAGIC                             train_local,
# MAGIC                             train_class,
# MAGIC                             train_priority,
# MAGIC                             train_pso,
# MAGIC                             train_perintis,/* sum ----------------------------- */ 
# MAGIC                             Sum(passenger) AS passenger ,
# MAGIC                             Sum(amount) AS amount,
# MAGIC                             Sum(distance) AS distance
# MAGIC                         FROM 
# MAGIC                             dashboard_ticketing_aggregate_system_posko_raw
# MAGIC                         WHERE 
# MAGIC                             id IS NOT NULL
# MAGIC                             AND Date(period_first_depart) BETWEEN '2023-04-12' AND '2023-05-03'
# MAGIC                         GROUP BY 
# MAGIC                             /* tambahan field ----------------------------- */ 
# MAGIC                             -- Extract(year FROM Date(period_first_depart)), 
# MAGIC                             year(period_first_depart),
# MAGIC                             /* default field ----------------------------- */ 
# MAGIC                             Date(period_first_depart),
# MAGIC                             train_number_merge ,
# MAGIC                             train_number,
# MAGIC                             train_name,
# MAGIC                             station_code_first_depart,
# MAGIC                             station_code_last_arrival,
# MAGIC                             distance_od,
# MAGIC                             time_first_depart,
# MAGIC                             time_last_arrival,
# MAGIC                             train_local,
# MAGIC                             train_class,
# MAGIC                             train_priority,
# MAGIC                             train_pso,
# MAGIC                             train_perintis
# MAGIC                 ) aggregate 
# MAGIC                 GROUP BY /* tambahan field ----------------------------- */ 
# MAGIC                 aggregate.year, /* default field group join biop ----------------------------- */ aggregate.date_first_depart,
# MAGIC                 aggregate.train_number_merge,
# MAGIC                 aggregate.train_number,
# MAGIC                 aggregate.train_name,
# MAGIC                 aggregate.train_class

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 
# MAGIC     data.year,
# MAGIC     Sum(data.train_count) train_count,
# MAGIC     Sum(data.passenger) passenger,
# MAGIC     Sum(data.amount) amount
# MAGIC FROM
# MAGIC     (
# MAGIC         SELECT 
# MAGIC             YEAR(period_first_depart) AS year,
# MAGIC             0 AS train_count,
# MAGIC             Sum(passenger) AS passenger,
# MAGIC             Sum(amount) AS amount
# MAGIC         FROM 
# MAGIC             dashboard_ticketing_aggregate_program_raw
# MAGIC         WHERE 
# MAGIC             id IS NOT NULL
# MAGIC             AND Date(period_first_depart) BETWEEN '2023-04-12' AND '2023-05-03'
# MAGIC         GROUP BY 
# MAGIC             YEAR(period_first_depart), 
# MAGIC             train_number
# MAGIC     ) data 
# MAGIC     GROUP BY year
# MAGIC ORDER BY year

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 
# MAGIC     * 
# MAGIC FROM
# MAGIC     dashboard_ticketing_aggregate_program_raw
# MAGIC WHERE 
# MAGIC     id IS NOT NULL
# MAGIC     AND Date(period_first_depart) BETWEEN '2023-04-12' AND '2023-05-03'
# MAGIC LIMIT 
# MAGIC     10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT distinct(period_first_depart) from dashboard_ticketing_aggregate_program_raw

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT period,
# MAGIC        Sum(train_count) AS train_count,
# MAGIC        Sum(passenger) AS passenger,
# MAGIC        Sum(amount) AS amount,
# MAGIC        Sum(distance) AS distance,
# MAGIC        Sum(capacity) AS capacity,
# MAGIC        Sum(capacity_stamform) AS capacity_stamform,
# MAGIC        Sum(seat) AS seat,
# MAGIC        Sum(biop) AS biop,
# MAGIC        Sum(fixed_cost) AS fixed_cost,
# MAGIC        Sum(variable_cost) AS variable_cost
# MAGIC FROM
# MAGIC     (SELECT 
# MAGIC         NULL AS period,
# MAGIC         0 AS train_count,
# MAGIC         0 AS passenger,
# MAGIC         0 AS amount,
# MAGIC         0 AS distance,
# MAGIC         0 AS capacity,
# MAGIC         0 AS capacity_stamform,
# MAGIC         0 AS seat,
# MAGIC         0 AS biop,
# MAGIC         0 AS fixed_cost,
# MAGIC         0 AS variable_cost
# MAGIC     FROM 
# MAGIC         dashboard_ticketing_aggregate_system_posko_raw
# MAGIC     WHERE 
# MAGIC         id IS NULL
# MAGIC     UNION ALL
# MAGIC         SELECT 
# MAGIC             period,
# MAGIC             0 AS train_count,
# MAGIC             Sum(passenger) AS passenger,
# MAGIC             Sum(amount) AS amount,
# MAGIC             Sum(distance) AS distance,
# MAGIC             Sum(capacity) AS capacity,
# MAGIC             Sum(capacity_stamform) AS capacity_stamform,
# MAGIC             Sum(seat) AS seat,
# MAGIC             0 AS biop,
# MAGIC             0 AS fixed_cost,
# MAGIC             0 AS variable_cost
# MAGIC          FROM
# MAGIC              (
# MAGIC                 SELECT 
# MAGIC                     /* tambahan field ----------------------------- */ 
# MAGIC                     aggregate.period, 
# MAGIC                     /* default field group join biop ----------------------------- */
# MAGIC                     aggregate.date_first_depart,
# MAGIC                     aggregate.train_number_merge,
# MAGIC                     aggregate.train_number,
# MAGIC                     aggregate.train_name,
# MAGIC                     aggregate.train_class,
# MAGIC                     /* sum ----------------------------- */ 
# MAGIC                     Sum(passenger) AS passenger,
# MAGIC                     Sum(amount) AS amount,
# MAGIC                     0 AS distance,
# MAGIC                     0 AS capacity,
# MAGIC                     0 AS capacity_stamform,
# MAGIC                     0 AS seat
# MAGIC               FROM
# MAGIC                   (
# MAGIC                     SELECT 
# MAGIC                         /* tambahan field ----------------------------- */ 
# MAGIC                         Date(period_first_depart) AS period, 
# MAGIC                         /* default field ----------------------------- */ 
# MAGIC                         Date (period_first_depart) AS date_first_depart,
# MAGIC                         train_number_merge,
# MAGIC                         train_number,
# MAGIC                         train_name,
# MAGIC                         station_code_first_depart,
# MAGIC                         station_code_last_arrival,
# MAGIC                         distance_od,
# MAGIC                         time_first_depart,
# MAGIC                         time_last_arrival,
# MAGIC                         train_local,
# MAGIC                         train_class,
# MAGIC                         train_priority,
# MAGIC                         train_pso,
# MAGIC                         train_perintis,
# MAGIC                         /* sum ----------------------------- */ 
# MAGIC                         Sum(passenger) AS passenger,
# MAGIC                         Sum(amount) AS amount,
# MAGIC                         Sum(distance) AS distance
# MAGIC                     FROM 
# MAGIC                         dashboard_ticketing_aggregate_system_posko_raw
# MAGIC                     WHERE 
# MAGIC                         id IS NOT NULL
# MAGIC                         AND Date(period_first_depart) BETWEEN '2023-04-12' AND '2023-05-03'
# MAGIC                     GROUP BY 
# MAGIC                         /* tambahan field ----------------------------- */ 
# MAGIC                         Date(period_first_depart), 
# MAGIC                         /* default field ----------------------------- */ 
# MAGIC                         Date(period_first_depart),
# MAGIC                         train_number_merge,
# MAGIC                         train_number,
# MAGIC                         train_name,
# MAGIC                         station_code_first_depart,
# MAGIC                         station_code_last_arrival,
# MAGIC                         distance_od,
# MAGIC                         time_first_depart,
# MAGIC                         time_last_arrival,
# MAGIC                         train_local,
# MAGIC                         train_class,
# MAGIC                         train_priority,
# MAGIC                         train_pso,
# MAGIC                         train_perintis
# MAGIC                     ) aggregate 
# MAGIC                     GROUP BY 
# MAGIC                     /* tambahan field ----------------------------- */ 
# MAGIC                     aggregate.period, 
# MAGIC                     /* default field group join biop ----------------------------- */ 
# MAGIC                     aggregate.date_first_depart,
# MAGIC                     aggregate.train_number_merge,
# MAGIC                     aggregate.train_number,
# MAGIC                     aggregate.train_name,
# MAGIC                     aggregate.train_class
# MAGIC                 ) data 
# MAGIC                 GROUP BY 
# MAGIC                     period
# MAGIC             ) program 
# MAGIC             GROUP BY 
# MAGIC             period
# MAGIC ORDER BY period ;

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

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 
# MAGIC     data.period,
# MAGIC     Sum(data.train_count) train_count,
# MAGIC     Sum(data.passenger) passenger,
# MAGIC     Sum(data.amount) amount
# MAGIC FROM
# MAGIC     (
# MAGIC         SELECT 
# MAGIC             Date(period_last_arrival) AS period,
# MAGIC             0 AS train_count,
# MAGIC             Sum(passenger) AS passenger,
# MAGIC             Sum(amount) AS amount
# MAGIC         FROM 
# MAGIC             dashboard_ticketing_aggregate_program_raw
# MAGIC         WHERE 
# MAGIC             id IS NOT NULL
# MAGIC             AND Date(period_last_arrival) BETWEEN '2023-04-12' AND '2023-05-03' 
# MAGIC             GROUP BY 
# MAGIC                 Date(period_last_arrival), 
# MAGIC                 train_number
# MAGIC                 ) data 
# MAGIC     GROUP BY period
# MAGIC ORDER BY period ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT distinct period_last_arrival from dashboard_ticketing_aggregate_program_raw where Date(period_last_arrival) BETWEEN '2023-04-12' AND '2023-05-03' 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT period,
# MAGIC        Sum(train_count) AS train_count,
# MAGIC        Sum(passenger) AS passenger,
# MAGIC        Sum(amount) AS amount,
# MAGIC        Sum(distance) AS distance,
# MAGIC        Sum(capacity) AS capacity,
# MAGIC        Sum(capacity_stamform) AS capacity_stamform,
# MAGIC        Sum(seat) AS seat,
# MAGIC        Sum(biop) AS biop,
# MAGIC        Sum(fixed_cost) AS fixed_cost,
# MAGIC        Sum(variable_cost) AS variable_cost
# MAGIC FROM
# MAGIC     (SELECT 
# MAGIC         NULL AS period,
# MAGIC         0 AS train_count,
# MAGIC         0 AS passenger,
# MAGIC         0 AS amount,
# MAGIC         0 AS distance,
# MAGIC         0 AS capacity,
# MAGIC         0 AS capacity_stamform,
# MAGIC         0 AS seat,
# MAGIC         0 AS biop,
# MAGIC         0 AS fixed_cost,
# MAGIC         0 AS variable_cost
# MAGIC     FROM 
# MAGIC         dashboard_ticketing_aggregate_system_posko_raw
# MAGIC     WHERE 
# MAGIC         id IS NULL
# MAGIC     UNION ALL
# MAGIC         SELECT 
# MAGIC             period,
# MAGIC             0 AS train_count,
# MAGIC             Sum(passenger) AS passenger,
# MAGIC             Sum(amount) AS amount,
# MAGIC             Sum(distance) AS distance,
# MAGIC             Sum(capacity) AS capacity,
# MAGIC             Sum(capacity_stamform) AS capacity_stamform,
# MAGIC             Sum(seat) AS seat,
# MAGIC             0 AS biop,
# MAGIC             0 AS fixed_cost,
# MAGIC             0 AS variable_cost
# MAGIC          FROM
# MAGIC              (
# MAGIC                 SELECT 
# MAGIC                     /* tambahan field ----------------------------- */ 
# MAGIC                     aggregate.period, 
# MAGIC                     /* default field group join biop ----------------------------- */
# MAGIC                     aggregate.date_first_depart,
# MAGIC                     aggregate.train_number_merge,
# MAGIC                     aggregate.train_number,
# MAGIC                     aggregate.train_name,
# MAGIC                     aggregate.train_class,
# MAGIC                     /* sum ----------------------------- */ 
# MAGIC                     Sum(passenger) AS passenger,
# MAGIC                     Sum(amount) AS amount,
# MAGIC                     0 AS distance,
# MAGIC                     0 AS capacity,
# MAGIC                     0 AS capacity_stamform,
# MAGIC                     0 AS seat
# MAGIC               FROM
# MAGIC                   (
# MAGIC                     SELECT 
# MAGIC                         /* tambahan field ----------------------------- */ 
# MAGIC                         Date(period_first_depart) AS period, 
# MAGIC                         /* default field ----------------------------- */ 
# MAGIC                         Date (period_first_depart) AS date_first_depart,
# MAGIC                         train_number_merge,
# MAGIC                         train_number,
# MAGIC                         train_name,
# MAGIC                         station_code_first_depart,
# MAGIC                         station_code_last_arrival,
# MAGIC                         distance_od,
# MAGIC                         time_first_depart,
# MAGIC                         time_last_arrival,
# MAGIC                         train_local,
# MAGIC                         train_class,
# MAGIC                         train_priority,
# MAGIC                         train_pso,
# MAGIC                         train_perintis,
# MAGIC                         /* sum ----------------------------- */ 
# MAGIC                         Sum(passenger) AS passenger,
# MAGIC                         Sum(amount) AS amount,
# MAGIC                         Sum(distance) AS distance
# MAGIC                     FROM 
# MAGIC                         dashboard_ticketing_aggregate_system_posko_raw
# MAGIC                     WHERE 
# MAGIC                         id IS NOT NULL
# MAGIC                         AND Date(period_first_depart) BETWEEN '2023-04-12' AND '2023-05-03'
# MAGIC                     GROUP BY 
# MAGIC                         /* tambahan field ----------------------------- */ 
# MAGIC                         Date(period_first_depart), 
# MAGIC                         /* default field ----------------------------- */ 
# MAGIC                         Date(period_first_depart),
# MAGIC                         train_number_merge,
# MAGIC                         train_number,
# MAGIC                         train_name,
# MAGIC                         station_code_first_depart,
# MAGIC                         station_code_last_arrival,
# MAGIC                         distance_od,
# MAGIC                         time_first_depart,
# MAGIC                         time_last_arrival,
# MAGIC                         train_local,
# MAGIC                         train_class,
# MAGIC                         train_priority,
# MAGIC                         train_pso,
# MAGIC                         train_perintis
# MAGIC                     ) aggregate 
# MAGIC                     GROUP BY 
# MAGIC                     /* tambahan field ----------------------------- */ 
# MAGIC                     aggregate.period, 
# MAGIC                     /* default field group join biop ----------------------------- */ 
# MAGIC                     aggregate.date_first_depart,
# MAGIC                     aggregate.train_number_merge,
# MAGIC                     aggregate.train_number,
# MAGIC                     aggregate.train_name,
# MAGIC                     aggregate.train_class
# MAGIC                 ) data 
# MAGIC                 GROUP BY 
# MAGIC                     period
# MAGIC             ) program 
# MAGIC             GROUP BY 
# MAGIC             period
# MAGIC ORDER BY period ;

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

# CELL ********************

df = spark.sql("SELECT * FROM data.dashboard_ticketing_aggregate_program_raw")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE aggregate_test AS
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM data.aggregate_test")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT
# MAGIC     Date(period_first_depart),
# MAGIC     area_first_depart, 
# MAGIC     area_code_first_depart, 
# MAGIC     area_name_first_depart,
# MAGIC     train_number_merge,
# MAGIC     train_number,
# MAGIC     train_name,
# MAGIC     station_first_depart, 
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
# MAGIC     -- sum(passenger) as total_passenger, 
# MAGIC     -- sum(amount) as total_amount
# MAGIC FROM
# MAGIC     dashboard_ticketing_aggregate_system_posko_raw
# MAGIC LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# %%sql
# 
# select *
# FROM
#     dashboard_ticketing_aggregate_system_posko_raw
# where time_last_arrival is not null
# limit 10

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select *
# MAGIC FROM
# MAGIC     dashboard_ticketing_aggregate_system_posko_raw
# MAGIC where time_last_arrival is not null and time_last_arrival <> ''
# MAGIC limit 10

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
