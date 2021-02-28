import findspark
findspark.init()
import glob
from .common import GenericETL

USER_ID = "userId"
MOVIE_ID = "movieId"
RATING = "rating"
TS = "timestamp"

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import IntegerType, FloatType, TimestampType
from pyspark.sql.functions import col, row_number, monotonically_increasing_id, mean, when, lit, lag


class KueskiChallengeETL(GenericETL):

    def run(self):

        session = SparkSession.builder.appName("KueskiRaitings").master("local[*]").config("spark.driver.memory", "8g").config("spark.executor.memory", "8g").getOrCreate()

        dataFrameReader = session.read

        df = dataFrameReader.option("header", "true").option("inferSchema", "true").csv(self.get_data())
        df = df.withColumn("index", row_number().over(Window.orderBy(monotonically_increasing_id()))-1)

        df = df.withColumn(USER_ID, df[USER_ID].cast(IntegerType()))
        df = df.withColumn(MOVIE_ID, df[MOVIE_ID].cast(IntegerType()))
        df = df.withColumn(RATING, df[RATING].cast(FloatType()))
        df = df.withColumn(TS, df[TS].cast(TimestampType()))

        w1 = Window.partitionBy(USER_ID).orderBy(TS)


        df = df.withColumn("nb_previous_ratings", row_number().over(w1)-1)
        df = df.withColumn("avg_ratings_previous", mean(RATING).over(w1))
        df = df.withColumn("avg_ratings_previous", lag('avg_ratings_previous', 1, 0).over(w1))

        obj = df.select([c for c in df.columns if c in ['nb_previous_ratings', 'avg_ratings_previous']])

        return obj.toPandas().to_json(orient="records")

    def get_data(self):
        return glob.glob(f"{self.data_source}/*.csv")[0]