from pyspark.sql.functions import col, current_timestamp,sum
from pyspark import pipelines as dp
from utilities import utils


source_path = spark.conf.get("source")

#bronze - ingest orders
@dp.table(name="orders_bronze_latest")
def orders_bronze():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{source_path}/orders")
    )
    return (
        df.withColumn("file_name", col("_metadata.file_path"))
          .withColumn("load_date", current_timestamp())
    )

#bronze - ingest customers
@dp.table(name="customers_bronze_latest")
def customers_bronze():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{source_path}/customers")
    )
    return (
        df.withColumn("file_name", col("_metadata.file_path"))
        .withColumn("load_date", current_timestamp())
    )

#orders_cleaned_silver - handle null or other constraint(expectations)
@dp.table(name="orders_cleaned_silver_latest")
@dp.expect_or_drop("valid_order", "order_id is not null")
@dp.expect_or_drop("valid_customer", "customer_id is not null")
def orders_cleaned_silver():
    df = spark.readStream.table("orders_bronze_latest")
    return (
        df.selectExpr(
        "OrderID as order_id",
        "OrderDate as order_date",
        "CustomerID as customer_id",
        "TotalAmount as total_amount",
        "Status as order_status",
        "file_name",
        "load_date"
    ).withColumn("total_amount_in_usd", utils.inr_to_usd(col("total_amount")))
    )

@dp.table(name="customers_cleaned_silver_latest")
@dp.expect_or_drop("valid_customer", "customer_id is not null")
def customers_cleaned_silver():
    df = spark.readStream.table("customers_bronze_latest")

    return(
        df.selectExpr(
            "CustomerID as customer_id",
            "CustomerName as customer_name",
            "ContactNumber as contact_number",
            "Email as email",
            "Address as city",
            "DateOfBirth as date_of_birth",
            "RegistrationDate as resgistraion_date"
            "file_name",
            "load_date"
        )
    )

#scd type for customer using AUTO CDC

dp.create_streaming_table("customers_silver_latest")

dp.create_auto_cdc_flow(
    target="customers_silver_latest",
    source="customers_cleaned_silver_latest",
    keys=["customer_id"],
    sequence_by="load_date",
    stored_as_scd_type=2
)

#scd 1 for orders

dp.create_streaming_table("orders_silver_latest")

dp.create_auto_cdc_flow(
    target="orders_silver_latest",
    source="orders_cleaned_silver_latest",
    keys=["order_id"],
    sequence_by="load_date",
    stored_as_scd_type=1
)

# create Gold layer - materialized view

@dp.materialized_view
def city_wise_sales():
    orders = spark.read.table("orders_silver_latest")
    customers = spark.read.table("customers_silver_latest")
    return (
        orders.join(customers, "customer_id")
        .groupBy("city")
        .agg(sum("total_amount").alias("total_sales"))
        )
