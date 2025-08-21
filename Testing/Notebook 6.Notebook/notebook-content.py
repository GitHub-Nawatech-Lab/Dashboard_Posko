# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "aea03f4b-8d44-4041-9115-dc5a092bea2b",
# META       "default_lakehouse_name": "adi_lakes",
# META       "default_lakehouse_workspace_id": "371caf18-1651-4320-b03d-ddd574e85e48",
# META       "known_lakehouses": [
# META         {
# META           "id": "aea03f4b-8d44-4041-9115-dc5a092bea2b"
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
# MAGIC CREATE FUNCTION dbo.CalcDiscount (@Price DECIMAL(10,2), @Rate DECIMAL(4,2))
# MAGIC RETURNS DECIMAL(10,2)
# MAGIC AS
# MAGIC BEGIN
# MAGIC     RETURN @Price * (1 - @DiscountRate)
# MAGIC END;
# MAGIC 
# MAGIC SELECT ProductName, Price, dbo.CalcDiscount(Price, 0.10) AS DiscountedPrice
# MAGIC FROM Products;

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
