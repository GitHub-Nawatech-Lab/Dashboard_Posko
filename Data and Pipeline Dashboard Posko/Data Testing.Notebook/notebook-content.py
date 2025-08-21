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
# MAGIC WITH cte AS (
# MAGIC   SELECT 
# MAGIC     *, 
# MAGIC     CASE 
# MAGIC     WHEN period_first_depart BETWEEN '2019-05-26' AND '2019-06-16' THEN 'Lebaran 2019' 
# MAGIC     WHEN period_first_depart BETWEEN '2020-05-24' AND '2020-06-14' THEN 'Lebaran 2020' 
# MAGIC     WHEN period_first_depart BETWEEN '2021-05-12' AND '2021-06-02' THEN 'Lebaran 2021' 
# MAGIC     WHEN period_first_depart BETWEEN '2022-04-22' AND '2022-05-13' THEN 'Lebaran 2022' 
# MAGIC     WHEN period_first_depart BETWEEN '2023-04-12' AND '2023-05-03' THEN 'Lebaran 2023' 
# MAGIC     WHEN period_first_depart BETWEEN '2024-03-31' AND '2024-04-21' THEN 'Lebaran 2024' 
# MAGIC     WHEN period_first_depart BETWEEN '2025-03-21' AND '2025-04-11' THEN 'Lebaran 2025' 
# MAGIC     WHEN period_first_depart BETWEEN '2026-03-21' AND '2026-04-11' THEN 'Lebaran 2026' 
# MAGIC     ELSE 'Hari Biasa' END AS Event,
# MAGIC     concat(train_name, " (" , train_number_merge , ")") as train_id  
# MAGIC   FROM 
# MAGIC     aggregate_system
# MAGIC ) 
# MAGIC SELECT 
# MAGIC   sum(total_passenger),
# MAGIC   sum(total_amount) 
# MAGIC FROM 
# MAGIC   cte a
# MAGIC LEFT JOIN dashboard_ticketing_stasiun_raw b 
# MAGIC   ON a.station_code_first_depart = b.c_stasiun_code 
# MAGIC LEFT JOIN dashboard_ticketing_train_raw c 
# MAGIC   ON a.train_id = c.train_id
# MAGIC WHERE 
# MAGIC   a.feeder = 0 and
# MAGIC   a.Event = 'Lebaran 2025'             --Lebaran 2024
# MAGIC   -- and a.area_name_first_depart = 'DAOP 1 JAKARTA' --DAOP 1 JAKARTA
# MAGIC --   and b.c_stasiun_name = ''         --PALMERAH
# MAGIC --   and c.train_number_merge = ''     --FEEDER (653) ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC WITH cte AS (
# MAGIC   SELECT 
# MAGIC     *, 
# MAGIC     CASE 
# MAGIC     WHEN period_first_depart BETWEEN '2019-05-26' AND '2019-06-16' THEN 'Lebaran 2019' 
# MAGIC     WHEN period_first_depart BETWEEN '2020-05-24' AND '2020-06-14' THEN 'Lebaran 2020' 
# MAGIC     WHEN period_first_depart BETWEEN '2021-05-12' AND '2021-06-02' THEN 'Lebaran 2021' 
# MAGIC     WHEN period_first_depart BETWEEN '2022-04-22' AND '2022-05-13' THEN 'Lebaran 2022' 
# MAGIC     WHEN period_first_depart BETWEEN '2023-04-12' AND '2023-05-03' THEN 'Lebaran 2023' 
# MAGIC     WHEN period_first_depart BETWEEN '2024-03-31' AND '2024-04-21' THEN 'Lebaran 2024' 
# MAGIC     WHEN period_first_depart BETWEEN '2025-03-21' AND '2025-04-11' THEN 'Lebaran 2025' 
# MAGIC     WHEN period_first_depart BETWEEN '2026-03-21' AND '2026-04-11' THEN 'Lebaran 2026' 
# MAGIC     ELSE 'Hari Biasa' END AS Event,
# MAGIC     concat(train_name, " (" , train_number_merge , ")") as train_id  
# MAGIC   FROM 
# MAGIC     aggregate_system
# MAGIC ) 
# MAGIC SELECT 
# MAGIC   event,
# MAGIC   a.period_first_depart,
# MAGIC   sum(total_passenger),
# MAGIC   sum(total_amount) 
# MAGIC FROM 
# MAGIC   cte a
# MAGIC LEFT JOIN dashboard_ticketing_stasiun_raw b 
# MAGIC   ON a.station_code_first_depart = b.c_stasiun_code 
# MAGIC LEFT JOIN dashboard_ticketing_train_raw c 
# MAGIC   ON a.train_id = c.train_id
# MAGIC LEFT JOIN dashboard_ticketing_area_raw d
# MAGIC   on a.area_code_first_depart = d.c_area_alias
# MAGIC WHERE 
# MAGIC   a.feeder = 0 and
# MAGIC   a.Event = 'Lebaran 2025'          --Lebaran 2024
# MAGIC   -- and d.c_area_name = 'DAOP 1 JAKARTA' --DAOP 1 JAKARTA
# MAGIC --   and b.c_stasiun_name = ''         --PALMERAH
# MAGIC --   and c.train_number_merge = ''     --FEEDER (653) ;
# MAGIC GROUP BY
# MAGIC   event, period_first_depart
# MAGIC ORDER BY
# MAGIC   event, period_first_depart

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC WITH cte AS (
# MAGIC   SELECT 
# MAGIC     *, 
# MAGIC     CASE 
# MAGIC     WHEN period_first_depart BETWEEN '2019-05-26' AND '2019-06-16' THEN 'Lebaran 2019' 
# MAGIC     WHEN period_first_depart BETWEEN '2020-05-24' AND '2020-06-14' THEN 'Lebaran 2020' 
# MAGIC     WHEN period_first_depart BETWEEN '2021-05-12' AND '2021-06-02' THEN 'Lebaran 2021' 
# MAGIC     WHEN period_first_depart BETWEEN '2022-04-22' AND '2022-05-13' THEN 'Lebaran 2022' 
# MAGIC     WHEN period_first_depart BETWEEN '2023-04-12' AND '2023-05-03' THEN 'Lebaran 2023' 
# MAGIC     WHEN period_first_depart BETWEEN '2024-03-31' AND '2024-04-21' THEN 'Lebaran 2024' 
# MAGIC     WHEN period_first_depart BETWEEN '2025-03-21' AND '2025-04-11' THEN 'Lebaran 2025' 
# MAGIC     WHEN period_first_depart BETWEEN '2026-03-21' AND '2026-04-11' THEN 'Lebaran 2026' 
# MAGIC     ELSE 'Hari Biasa' END AS Event,
# MAGIC     concat(train_name, " (" , train_number_merge , ")") as train_id  
# MAGIC   FROM 
# MAGIC     aggregate_system
# MAGIC ) 
# MAGIC SELECT 
# MAGIC   event,
# MAGIC   a.area_name_first_depart,
# MAGIC   sum(total_passenger),
# MAGIC   sum(total_amount) 
# MAGIC FROM 
# MAGIC   cte a
# MAGIC LEFT JOIN dashboard_ticketing_stasiun_raw b 
# MAGIC   ON a.station_code_first_depart = b.c_stasiun_code 
# MAGIC LEFT JOIN dashboard_ticketing_train_raw c 
# MAGIC   ON a.train_id = c.train_id
# MAGIC LEFT JOIN dashboard_ticketing_area_raw d
# MAGIC   on a.area_code_first_depart = d.c_area_alias
# MAGIC WHERE 
# MAGIC   -- a.area_name_first_depart is null and
# MAGIC   a.feeder = 0 and
# MAGIC   a.Event = 'Lebaran 2025'          --Lebaran 2024
# MAGIC   -- and d.c_area_name = 'DAOP 1 JAKARTA' --DAOP 1 JAKARTA
# MAGIC --   and b.c_stasiun_name = ''         --PALMERAH
# MAGIC --   and c.train_number_merge = ''     --FEEDER (653) ;
# MAGIC GROUP BY
# MAGIC   event, a.area_name_first_depart
# MAGIC ORDER BY
# MAGIC   event, a.area_name_first_depart

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT sum(total_passenger) from
# MAGIC aggregate_system as a
# MAGIC -- LEFT JOIN dashboard_ticketing_area_raw d
# MAGIC --   on a.area_code_first_depart = d.c_area_alias
# MAGIC LEFT JOIN dashboard_ticketing_stasiun_raw b 
# MAGIC   ON a.station_code_first_depart = b.c_stasiun_code 
# MAGIC LEFT JOIN dashboard_ticketing_area_raw d
# MAGIC   on b.c_area_id = d.c_area_id
# MAGIC where period_first_depart between '2025-03-21' and '2025-04-11' and feeder=0 and area_name_first_depart = 'DAOP 8 SURABAYA'  and d.c_area_name is null

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
