# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "54012a88-41af-4d0d-8c60-85f16ee6b00a",
# META       "default_lakehouse_name": "LH_test",
# META       "default_lakehouse_workspace_id": "19c4e82b-818b-4a84-b808-970799024b13",
# META       "known_lakehouses": [
# META         {
# META           "id": "54012a88-41af-4d0d-8c60-85f16ee6b00a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Lecture brute CSV
df_bronze = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("Files/Chocolate_Sales.csv")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = (
    df_bronze
        .withColumnRenamed("Sales Person", "sales_person")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("Product", "product")
        .withColumnRenamed("Date", "date")
        .withColumnRenamed("Amount", "amount")
        .withColumnRenamed("Boxes Shipped", "boxes_shipped")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze.write.format("delta").mode("overwrite").save("Tables/bronze_chocolate_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
