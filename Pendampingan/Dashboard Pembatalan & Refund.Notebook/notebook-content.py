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
# MAGIC INSERT INTO RTSNG_T_REFUND_REPORT
# MAGIC SELECT
# MAGIC     c.C_CANCELLATION_DATE AS CANCEL_TIME,
# MAGIC     r.C_REFUND_PAYSTARTDATE AS REFUND_START_DATE,
# MAGIC     r.C_REFUND_PAYENDDATE AS REFUND_END_DATE,
# MAGIC     r.C_REFUND_PAYDATE AS PAY_DATE,
# MAGIC     c.C_TRANSACTION_BOOKCODE AS BOOKING_CODE,
# MAGIC     c.C_TRANSACTIONDET_TICKETNUM AS TICKET_NUMBER,
# MAGIC     c.C_CANCELLATION_CANCELNUM AS CANCEL_NUMBER,
# MAGIC     c.C_CANCELLATION_NOKA AS TRAIN_NUMBER,
# MAGIC     c.C_CANCELLATION_TRAINNAME AS TRAIN_NAME,
# MAGIC     CONCAT(c.C_STASIUN_CODEORG, ' - ', c.C_STASIUN_CODEDES) AS ROUTE,
# MAGIC     c.C_WAGONCLASS_CODE AS WAGON_CLASS,
# MAGIC     c.C_TRIP_DATE AS TRIP_DATE,
# MAGIC     st_cancel.C_STASIUN_CODE AS CANCEL_STATION,
# MAGIC     SPLIT(shift_cancel.C_SHIFT_NAME, '\\|')[1] AS CANCEL_PIC,
# MAGIC     st_refund.C_STASIUN_CODE AS REFUND_STATION,
# MAGIC     r.C_UNIT_CODE AS REFUND_UNIT,
# MAGIC     SPLIT(shift_refund.C_SHIFT_NAME, '\\|')[1] AS REFUND_PIC,
# MAGIC     CASE 
# MAGIC         WHEN r.C_REFUND_PAYTYPE = 'T' THEN 'Transfer'
# MAGIC         WHEN r.C_REFUND_PAYTYPE = 'C' THEN 'Cash'
# MAGIC         ELSE ''
# MAGIC     END AS PAYMENT_TYPE,
# MAGIC     ct.C_CANCELLATIONTYPE_NAME AS REFUND_TYPE,
# MAGIC     c.C_CANCELLATION_AMOUNTTICKET AS TICKET_AMOUNT,
# MAGIC     r.C_REFUND_TOTAMOUNT AS REFUND_AMOUNT,
# MAGIC     CASE 
# MAGIC         WHEN r.C_REFUND_STATUS = '1' THEN 'Refunded'
# MAGIC         WHEN r.C_REFUND_STATUS = '0' THEN 'Not Refunded'
# MAGIC         ELSE ''
# MAGIC     END AS STATUS,
# MAGIC     cr.C_CANCELREASON_DESC AS REASON
# MAGIC FROM RTSNG_T_CANCELLATION c 
# MAGIC LEFT JOIN RTSNG_T_REFUND r ON c.C_CANCELLATION_CANCELNUM = r.C_CANCELLATION_CANCELNUM
# MAGIC LEFT JOIN RTSNG_T_CANCELREASON cr ON c.C_CANCELREASON_ID = cr.C_CANCELREASON_ID
# MAGIC LEFT JOIN RTSNG_T_CANCELLATIONTYPE ct ON c.C_CANCELLATIONTYPE_ID = ct.C_CANCELLATIONTYPE_ID
# MAGIC LEFT JOIN RTSNG_T_SHIFT shift_cancel ON c.C_SHIFT_ID = shift_cancel.C_SHIFT_ID
# MAGIC LEFT JOIN RTSNG_T_SHIFT shift_refund ON r.C_SHIFT_ID = shift_refund.C_SHIFT_ID
# MAGIC LEFT JOIN RTSNG_T_STASIUN st_cancel ON st_cancel.C_STASIUN_ID = c.C_STASIUN_ID
# MAGIC LEFT JOIN RTSNG_T_STASIUN st_refund ON st_refund.C_STASIUN_ID = r.C_STASIUN_ID
# MAGIC WHERE CAST(c.C_CANCELLATION_DATE AS DATE) >= CURRENT_DATE - 30 ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
