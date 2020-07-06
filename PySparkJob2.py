from __future__ import print_function

import argparse
from typing import Tuple, Sequence

from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession, DataFrame, Window, types, functions as F


def calc_features(spark_session: SparkSession) -> DataFrame:
    df: DataFrame = spark_session \
        .read \
        .option("header", True) \
        .option("inferSchema", False) \
        .csv("daily-raw") \
        .withColumn("closingPrice", F.col("closingPrice").cast(types.DoubleType()))

    def days(x: int) -> int:
        return x * 24 * 3600

    window = Window.partitionBy("ISIN").orderBy("Date")
    window2 = Window.partitionBy("ISIN", "localMin", "localMax").orderBy("Date")

    window3 = Window \
        .partitionBy("ISIN") \
        .orderBy(F.col("Date").cast("timestamp").cast("long")) \
        .rangeBetween(days(-31), days(-1))

    label_window = Window \
        .partitionBy("ISIN") \
        .orderBy(F.col("Date").cast("timestamp").cast("long")) \
        .rangeBetween(days(1), days(30))

    # TODO: show case a pandas UDF
    # https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html
    # https://docs.databricks.com/spark/latest/spark-sql/udf-python-pandas.html
    # https://spark.apache.org/docs/latest/api/python/pyspark.sql.html
    # https://stackoverflow.com/questions/40006395/applying-udfs-on-groupeddata-in-pyspark-with-functioning-python-example
    # https://intellipaat.com/community/11611/applying-udfs-on-groupeddata-in-pyspark-with-functioning-python-example

    @F.udf(types.StructType([types.StructField("Date", types.StringType(), True),
                             types.StructField("closingPrice", types.DoubleType(), True)]))
    def my_udf(entries: Sequence[Tuple[str, float]]):
        return min([e for e in entries if e[1] >= 65] + [entries[-1]], key=lambda x: x[0])

    df2: DataFrame = df \
        .withColumn("availableDays", F.datediff("Date", F.min("Date").over(window))) \
        .withColumn("label", F.collect_list(F.struct(F.col("Date"), F.col("closingPrice"))).over(label_window)) \
        .filter(F.size("label") > 0) \
        .withColumn("label", my_udf(F.col("label"))) \
        .withColumn("sellAt", F.col("label.Date")) \
        .withColumn("sellPrice", F.col("label.closingPrice")) \
        .drop("label") \
        .withColumn("diffToPrev", F.col("closingPrice") / F.lag("closingPrice", 1).over(window) - 1) \
        .withColumn("up", F.col("diffToPrev") >= 0) \
        .withColumn("change", F.col("up") != F.lag("up", 1).over(window)) \
        .withColumn("nextChange", F.lead("change", 1, False).over(window)) \
        .withColumn("localMax", F.col("up") & F.col("nextChange")) \
        .withColumn("localMin", ~F.col("up") & F.col("nextChange")) \
        .drop("up", "change", "nextChange") \
        .withColumn("index", F.row_number().over(window)) \
        .filter(F.col("localMin") | F.col("localMax")) \
        .withColumn("higher", F.col("closingPrice") >= F.lag("closingPrice", 1).over(window2)) \
        .withColumn("daysBetween", F.col("index") - F.lag("index", 1).over(window)) \
        .drop("index") \
        .withColumn("hallo", F.concat_ws(",", "localMax", "localMin", "higher"))

    indexer = StringIndexer(inputCol="hallo", outputCol="categoryIndex")

    df3: DataFrame = indexer.fit(df2).transform(df2) \
        .withColumn("hallo", F.format_string("%.0fx", "categoryIndex")) \
        .filter(F.col("higher").isNotNull()) \
        .withColumn("events", F.flatten(F.collect_list(F.array("daysBetween", "hallo")).over(window3))) \
        .drop("hallo", "categoryIndex") \
        .filter(F.col("availableDays") >= 30) \
        .orderBy("Date")

    return df3


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate stock trading features.')
    parser.add_argument('--out', dest="output_path", help='where output is stored')
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    df = calc_features(spark_session=spark)
    df.show(truncate=False)
