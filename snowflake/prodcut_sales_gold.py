from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sum as sum_, when

session = get_active_session()

orders = session.table("training.clean.orders_clean")
products = session.table("training.raw.products").select(
    "product_id", "product_name", "category"
)

product_sales = (
    orders
    .join(products, on="product_id", how="inner")
    .group_by("product_id", "product_name", "category")
    .agg(sum_(col("quantity") * col("unit_price")).alias("revenue"))
    .sort(col("revenue").desc())
)

product_sales.show(10)

product_sales.write.mode("overwrite").save_as_table("training.gold.product_sales_snowpark")