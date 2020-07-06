import unittest
from pyspark import SparkConf
from pyspark.sql import SparkSession
import PySparkJob, PySparkJob2


class PySparkJobTestCase(unittest.TestCase):

    def setUp(self):
        conf = SparkConf() \
            .setMaster("local[*]") \
            .setAppName("pyspark-local-test") \
            .setExecutorEnv("JAVA_HOME", "/usr/lib/jvm/oracle-java8-jdk-amd64") \
            .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .set("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
            .set("spark.driver.memory", "5g") \
            .set("spark.driver.extraJavaOptions",
                 "-Dlog4j.configuration=file:///home/ph/Projects/pyspark-cookbook/log4j.properties")

        self.spark_session = SparkSession.builder.config(conf=conf).getOrCreate()
        self.spark_session.sparkContext.setLogLevel("INFO")
        print(self.spark_session._jvm.org.apache.hadoop.util.VersionInfo.getVersion())

    def test_feature_generation(self):
        df = PySparkJob2.calc_features(spark_session=self.spark_session)
        df.show(truncate=False)
        df.printSchema()

    def test_preprocessing(self):
        PySparkJob.preprocess2(spark_session=self.spark_session, output_path="test-test", date_filter="2020-*")
