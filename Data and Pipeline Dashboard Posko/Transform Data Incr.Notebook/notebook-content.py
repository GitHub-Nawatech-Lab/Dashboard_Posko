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
# META       "default_lakehouse_workspace_id": "371caf18-1651-4320-b03d-ddd574e85e48"
# META     }
# META   }
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
# MAGIC DELETE FROM aggregate_system WHERE period_first_depart BETWEEN '2025-03-21' AND '2025-04-11' AND feeder = 0

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO aggregate_system
# MAGIC SELECT
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
# MAGIC         0 as feeder,
# MAGIC         sum(passenger) as total_passenger, 
# MAGIC         sum(amount) as total_amount,
# MAGIC         CURRENT_TIMESTAMP() AS job_running,
# MAGIC         max(last_update) as cut_off_data
# MAGIC     FROM
# MAGIC         dashboard_ticketing_aggregate_system_incr
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
# MAGIC         -- job_running
# MAGIC         -- cut_off_data
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

# MAGIC %%sql
# MAGIC 
# MAGIC DELETE FROM dashboard_ticketing_aggregate_system_incr

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DELETE FROM aggregate_system WHERE period_first_depart BETWEEN '2025-03-21' AND '2025-04-11' AND feeder = 1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO aggregate_system 
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
# MAGIC         trim(train_class),
# MAGIC         train_priority,
# MAGIC         train_pso,
# MAGIC         train_perintis,
# MAGIC         1 as feeder,
# MAGIC         sum(passenger) as total_passenger, 
# MAGIC         sum(amount) as total_amount,
# MAGIC         CURRENT_TIMESTAMP() AS job_running,
# MAGIC         max(last_update) as cut_off_data
# MAGIC     FROM
# MAGIC         dashboard_ticketing_aggregate_system_feeder_raw
# MAGIC     WHERE 
# MAGIC         company in (0, 2)
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

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
