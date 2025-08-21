# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a5206e31-7da5-4f3d-a24e-6a1b04378a62",
# META       "default_lakehouse_name": "Dashboard_RTS",
# META       "default_lakehouse_workspace_id": "371caf18-1651-4320-b03d-ddd574e85e48",
# META       "known_lakehouses": [
# META         {
# META           "id": "a5206e31-7da5-4f3d-a24e-6a1b04378a62"
# META         }
# META       ]
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
# MAGIC SELECT 
# MAGIC     dbo.fn_GetDeparture(13) AS DEPARTURE,
# MAGIC FROM RTSNG_T_CANCELLATION 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
