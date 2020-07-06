from __future__ import print_function

import argparse
from functools import reduce
from typing import Sequence, Optional

from pyspark.sql import SparkSession, DataFrame, functions as F


def preprocess2(spark_session: SparkSession, output_path: Optional[str], date_filter: str) -> None:
    log4j = spark_session._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger(__name__)

    logger.info("Preprocessing start")
    path = spark_session.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
    file_system = spark_session.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem
    prefix = "s3a://deutsche-boerse-xetra-pds"

    fs = file_system.get(
        path(prefix).toUri(),
        spark_session._jsc.hadoopConfiguration()
    )

    glob_status = fs.globStatus(path(f"{prefix}/{date_filter}"))
    paths = [x.getPath() for x in glob_status]

    # print(*[p.getName() for p in paths], sep=",")
    logger.info(
        f"Reading the following days from {prefix}: {', '.join([p.getName() for p in paths])}"
    )

    def path_to_df(path: str, date: str) -> DataFrame:
        return spark_session \
            .read \
            .option("header", True) \
            .option("inferSchema", False) \
            .csv(path) \
            .filter(F.col("ISIN").isin("DE000ZAL1111", "DE000A12DM80")) \
            .groupBy("ISIN") \
            .agg(F.sum("TradedVolume").alias("volume"),
                 F.sum("NumberOfTrades").alias("trades"),
                 F.max(F.array("Time", "EndPrice"))[1].alias("closingPrice")
                 ) \
            .withColumn("Date", F.lit(date)) \
            .coalesce(1)

    def add_next_df(acc: DataFrame, next_df: DataFrame) -> DataFrame:
        return acc.unionAll(next_df)

    def union_all(*dfs: DataFrame) -> DataFrame:
        return reduce(add_next_df, dfs)  # DataFrame.unionAll

    # Since Python 3.8 consider assignment expressions: https://stackoverflow.com/a/55890523
    daily_data: Sequence[DataFrame] = [path_to_df(str(p), p.getName()) for p in paths]
    df: DataFrame = union_all(*daily_data)
    df.explain(mode="simple")
    print(df.rdd.toDebugString().decode('UTF-8'))
    logger.info(f"Total count is {df.rdd.count()} with {df.rdd.getNumPartitions()} partitions)")

    if output_path:
        df \
            .coalesce(1) \
            .orderBy("ISIN", "Date") \
            .write \
            .option("header", True) \
            .option("nullValue", None) \
            .mode("overwrite") \
            .csv(output_path)

        logger.info("Preprocessing done, output successfully saved.")
    else:
        df.orderBy("Date").show(truncate=False)
        logger.info("Preprocessing done")


def preprocess(spark_session: SparkSession, output_path: Optional[str], date_filter: str) -> None:
    log4j = spark_session._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger(__name__)
    logger.info("Preprocessing start")

    df: DataFrame = spark_session \
        .read \
        .option("header", True) \
        .option("inferSchema", False) \
        .csv(f"s3a://deutsche-boerse-xetra-pds/{date_filter}") \
        .filter(F.col("ISIN").isin("DE000ZAL1111", "DE000A12DM80")) \
        .groupBy("Date", "ISIN") \
        .agg(F.sum("TradedVolume").alias("volume"),
             F.sum("NumberOfTrades").alias("trades"),
             F.max(F.array("Time", "EndPrice"))[1].alias("closingPrice")
             )

    df.explain()

    if output_path:
        df \
            .orderBy("ISIN", "Date") \
            .coalesce(1) \
            .write \
            .option("header", True) \
            .option("nullValue", None) \
            .mode("overwrite") \
            .csv(output_path)

        logger.info("Preprocessing done, output successfully saved.")
    else:
        df.orderBy("Date").show(truncate=False)
        logger.info("Preprocessing done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Aggregate stock trades to daily.')
    parser.add_argument('--out', dest="output_path", help='path where output is stored')
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    preprocess(spark_session=spark, output_path=args.output_path)
