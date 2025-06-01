from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

APP_NAME = "DWHtoClickHouseReports"
POSTGRES_JDBC_JAR_PATH = "/opt/spark/jars/postgresql-42.6.0.jar"
CLICKHOUSE_JDBC_JAR_PATH = "/opt/spark/jars/clickhouse-jdbc-0.4.6.jar"

PG_DB_URL = "jdbc:postgresql://postgres_db:5432/bigdata"
PG_DB_PROPERTIES = {
    "user": "foxiq",
    "password": "915286",
    "driver": "org.postgresql.Driver"
}

CH_DB_URL = "jdbc:clickhouse://clickhouse:8123/default"
CH_DB_PROPERTIES = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}

CH_WRITE_MODE = "overwrite"
CH_TABLE_ENGINE_OPTIONS = "ENGINE = MergeTree() ORDER BY tuple()"

def initialize_spark_session(app_name, pg_jar, ch_jar):
    jars_path = f"{pg_jar},{ch_jar}"
    session = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", jars_path)
        .getOrCreate()
    )
    return session


def load_dwh_table(spark, table_name):
    df = (
        spark.read
        .format("jdbc")
        .option("url", PG_DB_URL)
        .option("dbtable", table_name)
        .options(**PG_DB_PROPERTIES)
        .load()
    )
    return df


def save_report_to_clickhouse(report_df, report_table_name, order_by_cols=None):
    current_ch_table_engine = CH_TABLE_ENGINE_OPTIONS
    if "MergeTree" in CH_TABLE_ENGINE_OPTIONS and order_by_cols:
        order_by_clause = ", ".join(order_by_cols)
        current_ch_table_engine = f"ENGINE = MergeTree() ORDER BY ({order_by_clause})"
    elif "MergeTree" in CH_TABLE_ENGINE_OPTIONS and not order_by_cols and "ORDER BY tuple()" not in CH_TABLE_ENGINE_OPTIONS:
        current_ch_table_engine = f"{CH_TABLE_ENGINE_OPTIONS.replace('ORDER BY tuple()', '')} ORDER BY tuple()"

    (
        report_df.write
        .format("jdbc")
        .mode(CH_WRITE_MODE)
        .option("url", CH_DB_URL)
        .option("dbtable", report_table_name)
        .option("createTableOptions", current_ch_table_engine)
        .options(**CH_DB_PROPERTIES)
        .save()
    )


def generate_reports(spark):
    fact_sales_df = load_dwh_table(spark, "fact_sales").cache()
    dim_product_df = load_dwh_table(spark, "dim_product").cache()
    dim_customer_df = load_dwh_table(spark, "dim_customer").cache()
    dim_date_df = load_dwh_table(spark, "dim_date").cache()
    dim_store_df = load_dwh_table(spark, "dim_store").cache()
    dim_supplier_df = load_dwh_table(spark, "dim_supplier").cache()

    report_top_10_products = (
        fact_sales_df.groupBy("product_sk")
        .agg(
            F.sum("sale_quantity").alias("total_quantity_sold"),
            F.sum("sale_total_price").alias("total_revenue_generated")
        )
        .join(dim_product_df, "product_sk")
        .select(
            dim_product_df.product_id.alias("business_product_id"),
            dim_product_df.name.alias("product_name"),
            dim_product_df.category.alias("product_category"),
            "total_quantity_sold",
            "total_revenue_generated"
        )
        .orderBy(F.desc("total_quantity_sold"))
        .limit(10)
    )
    save_report_to_clickhouse(report_top_10_products, "mart_top_10_selling_products",
                              order_by_cols=["total_quantity_sold"])

    report_revenue_by_category = (
        fact_sales_df.join(dim_product_df, "product_sk")
        .groupBy(dim_product_df.category.alias("product_category"))
        .agg(F.sum("sale_total_price").alias("category_total_revenue"))
        .orderBy(F.desc("category_total_revenue"))
    )
    save_report_to_clickhouse(report_revenue_by_category, "mart_revenue_by_product_category",
                              order_by_cols=["product_category"])

    report_product_feedback_summary = dim_product_df.select(
        dim_product_df.product_id.alias("business_product_id"),
        dim_product_df.name.alias("product_name"),
        dim_product_df.category.alias("product_category"),
        dim_product_df.rating.alias("average_rating"),
        dim_product_df.reviews.alias("number_of_reviews")
    ).orderBy(F.desc("number_of_reviews"))
    save_report_to_clickhouse(report_product_feedback_summary, "mart_product_feedback_summary",
                              order_by_cols=["business_product_id"])

    report_top_10_customers_by_purchase = (
        fact_sales_df.groupBy("customer_sk")
        .agg(F.sum("sale_total_price").alias("customer_total_purchases"))
        .join(dim_customer_df, "customer_sk")
        .select(
            dim_customer_df.customer_id.alias("business_customer_id"),
            dim_customer_df.first_name,
            dim_customer_df.last_name,
            dim_customer_df.country.alias("customer_country"),
            "customer_total_purchases"
        )
        .orderBy(F.desc("customer_total_purchases"))
        .limit(10)
    )
    save_report_to_clickhouse(report_top_10_customers_by_purchase, "mart_top_10_customers_by_purchase",
                              order_by_cols=["customer_total_purchases"])

    report_customer_distribution_by_country = (
        dim_customer_df.groupBy(dim_customer_df.country.alias("customer_country"))
        .agg(F.countDistinct(dim_customer_df.customer_id).alias("distinct_customer_count"))
        .orderBy(F.desc("distinct_customer_count"))
    )
    save_report_to_clickhouse(report_customer_distribution_by_country, "mart_customer_distribution_by_country",
                              order_by_cols=["customer_country"])

    report_avg_customer_order_value = (
        fact_sales_df.groupBy("customer_sk")
        .agg(
            (F.sum("sale_total_price") / F.countDistinct("sale_sk")).alias("avg_transaction_value_per_customer")
        )
        .join(dim_customer_df, "customer_sk")
        .select(
            dim_customer_df.customer_id.alias("business_customer_id"),
            dim_customer_df.first_name,
            dim_customer_df.last_name,
            "avg_transaction_value_per_customer"
        )
        .orderBy(F.desc("avg_transaction_value_per_customer"))
    )
    save_report_to_clickhouse(report_avg_customer_order_value, "mart_avg_customer_order_value",
                              order_by_cols=["business_customer_id"])

    sales_with_date_details_df = fact_sales_df.join(dim_date_df, "date_sk")

    report_monthly_sales_trends = (
        sales_with_date_details_df
        .groupBy(dim_date_df.year, dim_date_df.month, dim_date_df.month_name)
        .agg(
            F.sum("sale_total_price").alias("monthly_total_revenue"),
            F.sum("sale_quantity").alias("monthly_total_quantity_sold")
        )
        .orderBy(dim_date_df.year, dim_date_df.month)
    )
    save_report_to_clickhouse(report_monthly_sales_trends, "mart_monthly_sales_trends", order_by_cols=["year", "month"])

    report_yearly_sales_trends = (
        sales_with_date_details_df
        .groupBy(dim_date_df.year)
        .agg(
            F.sum("sale_total_price").alias("yearly_total_revenue"),
            F.sum("sale_quantity").alias("yearly_total_quantity_sold")
        )
        .orderBy(dim_date_df.year)
    )
    save_report_to_clickhouse(report_yearly_sales_trends, "mart_yearly_sales_trends", order_by_cols=["year"])

    window_spec_mom = Window.orderBy("year", "month")
    report_mom_revenue_comparison = (
        report_monthly_sales_trends
        .withColumn("previous_month_revenue", F.lag("monthly_total_revenue", 1, 0).over(window_spec_mom))
        .withColumn(
            "mom_revenue_change",
            (F.col("monthly_total_revenue") - F.col("previous_month_revenue"))
        )
        .withColumn(
            "mom_revenue_change_percent",
            F.when(F.col("previous_month_revenue") != 0,
                   F.round((F.col("mom_revenue_change") / F.col("previous_month_revenue")) * 100, 2)
                   ).otherwise(0)
        )
        .select(
            "year", "month", "month_name",
            "monthly_total_revenue",
            "previous_month_revenue",
            "mom_revenue_change",
            "mom_revenue_change_percent"
        )
    )
    save_report_to_clickhouse(report_mom_revenue_comparison, "mart_mom_revenue_comparison",
                              order_by_cols=["year", "month"])

    report_avg_order_size_by_month = (
        sales_with_date_details_df
        .groupBy(dim_date_df.year, dim_date_df.month, dim_date_df.month_name)
        .agg(
            (F.sum("sale_total_price") / F.countDistinct("sale_sk")).alias("avg_monthly_order_size")
        )
        .orderBy(dim_date_df.year, dim_date_df.month)
    )
    save_report_to_clickhouse(report_avg_order_size_by_month, "mart_avg_order_size_by_month",
                              order_by_cols=["year", "month"])

    sales_with_store_details_df = fact_sales_df.join(dim_store_df, "store_sk")

    report_top_5_stores_by_revenue = (
        sales_with_store_details_df.groupBy(dim_store_df.name.alias("store_name"), dim_store_df.city,
                                            dim_store_df.country)
        .agg(F.sum("sale_total_price").alias("store_total_revenue"))
        .orderBy(F.desc("store_total_revenue"))
        .limit(5)
    )
    save_report_to_clickhouse(report_top_5_stores_by_revenue, "mart_top_5_stores_by_revenue",
                              order_by_cols=["store_total_revenue"])

    report_sales_by_store_location = (
        sales_with_store_details_df
        .groupBy(dim_store_df.city.alias("store_city"), dim_store_df.country.alias("store_country"))
        .agg(
            F.sum("sale_total_price").alias("location_total_revenue"),
            F.sum("sale_quantity").alias("location_total_quantity_sold")
        )
        .orderBy(F.desc("location_total_revenue"))
    )
    save_report_to_clickhouse(report_sales_by_store_location, "mart_sales_by_store_location",
                              order_by_cols=["store_country", "store_city"])

    report_avg_store_order_value = (
        sales_with_store_details_df
        .groupBy(dim_store_df.name.alias("store_name"))
        .agg(
            (F.sum("sale_total_price") / F.countDistinct("sale_sk")).alias("avg_transaction_value_per_store")
        )
        .orderBy(F.desc("avg_transaction_value_per_store"))
    )
    save_report_to_clickhouse(report_avg_store_order_value, "mart_avg_store_order_value", order_by_cols=["store_name"])

    sales_with_supplier_details_df = fact_sales_df.join(dim_supplier_df, "supplier_sk")

    report_top_5_suppliers_by_revenue = (
        sales_with_supplier_details_df
        .groupBy(dim_supplier_df.name.alias("supplier_name"), dim_supplier_df.country.alias("supplier_country"))
        .agg(F.sum("sale_total_price").alias("supplier_generated_revenue"))
        .orderBy(F.desc("supplier_generated_revenue"))
        .limit(5)
    )
    save_report_to_clickhouse(report_top_5_suppliers_by_revenue, "mart_top_5_suppliers_by_revenue",
                              order_by_cols=["supplier_generated_revenue"])

    report_avg_product_price_by_supplier = (
        fact_sales_df
        .join(dim_product_df,
              "product_sk")
        .join(dim_supplier_df, "supplier_sk")
        .groupBy(dim_supplier_df.name.alias("supplier_name"))
        .agg(F.avg("transaction_unit_price").alias("avg_sold_unit_price_from_supplier"))
        .orderBy(F.desc("avg_sold_unit_price_from_supplier"))
    )
    save_report_to_clickhouse(report_avg_product_price_by_supplier, "mart_avg_product_price_by_supplier",
                              order_by_cols=["supplier_name"])

    report_sales_by_supplier_country = (
        sales_with_supplier_details_df
        .groupBy(dim_supplier_df.country.alias("supplier_country"))
        .agg(
            F.sum("sale_total_price").alias("country_total_revenue"),
            F.sum("sale_quantity").alias("country_total_quantity_sold")
        )
        .orderBy(F.desc("country_total_revenue"))
    )
    save_report_to_clickhouse(report_sales_by_supplier_country, "mart_sales_by_supplier_country",
                              order_by_cols=["supplier_country"])

    report_highest_rated_products = (
        dim_product_df.select(
            dim_product_df.product_id.alias("business_product_id"),
            dim_product_df.name.alias("product_name"),
            dim_product_df.rating
        )
        .orderBy(F.desc_nulls_last("rating"))
        .limit(10)
    )
    save_report_to_clickhouse(report_highest_rated_products, "mart_highest_rated_products", order_by_cols=["rating"])

    report_lowest_rated_products = (
        dim_product_df.select(
            dim_product_df.product_id.alias("business_product_id"),
            dim_product_df.name.alias("product_name"),
            dim_product_df.rating
        )
        .filter(F.col("rating").isNotNull())
        .orderBy(F.asc_nulls_first("rating"))
        .limit(10)
    )
    save_report_to_clickhouse(report_lowest_rated_products, "mart_lowest_rated_products", order_by_cols=["rating"])

    product_sales_and_rating_df = (
        fact_sales_df.join(dim_product_df, "product_sk")
        .groupBy(
            dim_product_df.product_id.alias("business_product_id"),
            dim_product_df.name.alias("product_name")
        )
        .agg(
            F.avg(dim_product_df.rating).alias("avg_product_rating"),
            F.sum(fact_sales_df.sale_quantity).alias("total_product_quantity_sold")
        )
        .filter(F.col("avg_product_rating").isNotNull() & (F.col("total_product_quantity_sold").isNotNull()))
    )

    if product_sales_and_rating_df.count() > 1:
        correlation_value = product_sales_and_rating_df.stat.corr("avg_product_rating", "total_product_quantity_sold")
    else:
        correlation_value = None

    correlation_df = spark.createDataFrame(
        [(correlation_value if correlation_value is not None else float('nan'),)],
        ["rating_sales_volume_correlation_coeff"]
    )
    save_report_to_clickhouse(correlation_df, "mart_rating_sales_correlation",
                              order_by_cols=[])

    report_top_reviewed_products = (
        dim_product_df.select(
            dim_product_df.product_id.alias("business_product_id"),
            dim_product_df.name.alias("product_name"),
            dim_product_df.reviews.alias("number_of_reviews")
        )
        .orderBy(F.desc_nulls_last("number_of_reviews"))
        .limit(10)
    )
    save_report_to_clickhouse(report_top_reviewed_products, "mart_top_reviewed_products",
                              order_by_cols=["number_of_reviews"])

    fact_sales_df.unpersist()
    dim_product_df.unpersist()
    dim_customer_df.unpersist()
    dim_date_df.unpersist()
    dim_store_df.unpersist()
    dim_supplier_df.unpersist()


if __name__ == "__main__":
    spark_session = initialize_spark_session(
        APP_NAME,
        POSTGRES_JDBC_JAR_PATH,
        CLICKHOUSE_JDBC_JAR_PATH
    )
    try:
        generate_reports(spark_session)
    finally:
        spark_session.stop()