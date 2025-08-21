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
# MAGIC DELETE FROM RTSNG_T_CANCELLATION WHERE CAST(C_CANCELLATION_CREATEDON AS DATE) >= CURRENT_DATE - 30

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM RTSNG_T_REFUND WHERE CAST(C_REFUND_CREATEDON AS DATE) >= CURRENT_DATE - 30

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DELETE FROM rtsng_t_refund_report WHERE CAST(CANCEL_TIME AS DATE) >= CURRENT_DATE - 30

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
