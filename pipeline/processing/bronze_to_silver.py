"""
BƯỚC 3: PySpark — Bronze → Silver
Làm sạch dữ liệu, validate, standardize.
Chạy qua Airflow hoặc trực tiếp:
docker exec spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0, org.apache.hadoop:hadoop-aws:3.3.4 pipeline/processing/bronze_to_silver.py
"""

import re
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, BooleanType


# ── Spark Session với Iceberg + MinIO ─────────────────────────
def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AirbnbBronzeToSilver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "s3a://iceberg-warehouse/")
        # MinIO / S3 config
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ── LISTINGS: Bronze → Silver ──────────────────────────────────
def clean_listings(df: DataFrame) -> DataFrame:
    """
    Làm sạch bảng Listings:
    - Ép kiểu đúng
    - Chuẩn hóa boolean (t/f → True/False)
    - Parse amenities từ string list
    - Lọc bỏ records null listing_id
    """
    cleaned = (
        df
        # Drop duplicates theo listing_id
        .dropDuplicates(["listing_id"])

        # Lọc record hợp lệ
        .filter(F.col("listing_id").isNotNull())
        .filter(F.col("city").isNotNull())

        # Ép kiểu số
        .withColumn("listing_id",        F.col("listing_id").cast(IntegerType()))
        .withColumn("host_id",           F.col("host_id").cast(IntegerType()))
        .withColumn("price",             F.col("price").cast(DoubleType()))
        .withColumn("accommodates",      F.col("accommodates").cast(IntegerType()))
        .withColumn("bedrooms",          F.col("bedrooms").cast(IntegerType()))
        .withColumn("minimum_nights",    F.col("minimum_nights").cast(IntegerType()))
        .withColumn("maximum_nights",    F.col("maximum_nights").cast(IntegerType()))
        .withColumn("host_total_listings_count", F.col("host_total_listings_count").cast(IntegerType()))
        .withColumn("latitude",          F.col("latitude").cast(DoubleType()))
        .withColumn("longitude",         F.col("longitude").cast(DoubleType()))

        # Review scores → double
        .withColumn("review_scores_rating",        F.col("review_scores_rating").cast(DoubleType()))
        .withColumn("review_scores_accuracy",      F.col("review_scores_accuracy").cast(DoubleType()))
        .withColumn("review_scores_cleanliness",   F.col("review_scores_cleanliness").cast(DoubleType()))
        .withColumn("review_scores_checkin",       F.col("review_scores_checkin").cast(DoubleType()))
        .withColumn("review_scores_communication", F.col("review_scores_communication").cast(DoubleType()))
        .withColumn("review_scores_location",      F.col("review_scores_location").cast(DoubleType()))
        .withColumn("review_scores_value",         F.col("review_scores_value").cast(DoubleType()))

        # Chuẩn hóa boolean: "t" → true, "f" → false
        .withColumn("host_is_superhost",       (F.col("host_is_superhost") == "t").cast(BooleanType()))
        .withColumn("host_has_profile_pic",    (F.col("host_has_profile_pic") == "t").cast(BooleanType()))
        .withColumn("host_identity_verified",  (F.col("host_identity_verified") == "t").cast(BooleanType()))
        .withColumn("instant_bookable",        (F.col("instant_bookable") == "t").cast(BooleanType()))

        # Parse host_response_rate: "97%" → 97.0
        .withColumn(
            "host_response_rate_pct",
            F.regexp_extract(F.col("host_response_rate"), r"(\d+)", 1).cast(DoubleType())
        )
        .withColumn(
            "host_acceptance_rate_pct",
            F.regexp_extract(F.col("host_acceptance_rate"), r"(\d+)", 1).cast(DoubleType())
        )

        # Đếm số amenities
        .withColumn(
            "amenity_count",
            F.size(F.split(F.regexp_replace(F.col("amenities"), r'[\[\]"]', ""), ","))
        )

        # Chuẩn hóa tên city
        .withColumn("city", F.initcap(F.trim(F.col("city"))))

        # Lọc price hợp lý (> 0 và < 10,000)
        .filter((F.col("price") > 0) & (F.col("price") < 10_000))

        # Thêm metadata
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_layer", F.lit("silver"))

        # Drop các cột raw không cần
        .drop("host_response_rate", "host_acceptance_rate")
    )
    return cleaned


# ── REVIEWS: Bronze → Silver ───────────────────────────────────
def clean_reviews(df: DataFrame) -> DataFrame:
    """
    Làm sạch bảng Reviews:
    - Ép kiểu
    - Parse date
    - Drop duplicates
    """
    cleaned = (
        df
        .dropDuplicates(["review_id"])
        .filter(F.col("review_id").isNotNull())
        .filter(F.col("listing_id").isNotNull())

        .withColumn("review_id",   F.col("review_id").cast(IntegerType()))
        .withColumn("listing_id",  F.col("listing_id").cast(IntegerType()))
        .withColumn("reviewer_id", F.col("reviewer_id").cast(IntegerType()))
        .withColumn("review_date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        .withColumn("review_year",  F.year(F.col("review_date")))
        .withColumn("review_month", F.month(F.col("review_date")))

        .withColumn("ingested_at", F.current_timestamp())
        .drop("date")
    )
    return cleaned


# ── MAIN ───────────────────────────────────────────────────────
def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("PySpark Bronze → Silver")
    print("=" * 60)

    # Đọc từ Bronze (JSON Lines trên MinIO)
    bronze_listings_path = "s3a://bronze/airbnb.listings.raw/"
    bronze_reviews_path  = "s3a://bronze/airbnb.reviews.raw/"

    print("\n📥 Reading Bronze Listings...")
    raw_listings = spark.read.option("multiline", "false").json(bronze_listings_path)
    print(f"   Raw count: {raw_listings.count():,}")

    print("\n📥 Reading Bronze Reviews...")
    raw_reviews = spark.read.option("multiline", "false").json(bronze_reviews_path)
    print(f"   Raw count: {raw_reviews.count():,}")

    # Transform
    print("\n🔄 Cleaning Listings...")
    silver_listings = clean_listings(raw_listings)
    print(f"   Cleaned count: {silver_listings.count():,}")

    print("\n🔄 Cleaning Reviews...")
    silver_reviews = clean_reviews(raw_reviews)
    print(f"   Cleaned count: {silver_reviews.count():,}")

    # Ensure namespace exists in Iceberg REST catalog before table writes
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")

    # Ghi vào Silver dưới dạng Iceberg table
    print("\n📤 Writing Silver Listings → Iceberg...")
    (
        silver_listings.writeTo("local.silver.listings")
        .tableProperty("format-version", "2")
        .partitionedBy(F.col("city"))           # partition theo city để query nhanh
        .createOrReplace()
    )

    print("\n📤 Writing Silver Reviews → Iceberg...")
    (
        silver_reviews.writeTo("local.silver.reviews")
        .tableProperty("format-version", "2")
        .partitionedBy(F.col("review_year"), F.col("review_month"))
        .createOrReplace()
    )

    print("\n✅ Bronze → Silver complete!")

    # Quick validation
    print("\n📊 Silver Listings sample:")
    spark.table("local.silver.listings").select(
        "listing_id", "city", "room_type", "price", "host_is_superhost",
        "review_scores_rating", "amenity_count"
    ).show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
