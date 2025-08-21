import sys
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, explode
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

AWS_BUCKET_NAME_INPUT = "weather-etl-madagascar/regions/data-input/"
AWS_BUCKET_NAME_OUTPUT = "weather-etl-madagascar/regions/data-output/"

s3 = boto3.resource('s3')
bucket_name_output = AWS_BUCKET_NAME_OUTPUT.split("/")[0]
bucket_output = s3.Bucket(bucket_name_output)

def transform_data():
    today_str = datetime.now().strftime('%Y%m%d')

    input_path = f"s3://{AWS_BUCKET_NAME_INPUT}/regions_{today_str}.json"
    df = spark.read.format("json") \
        .option("multiline", "true") \
        .option("inferSchema", "true") \
        .load(input_path)

    df_days = df.withColumn("day", explode(col("days")))

    flat_df = df_days.select(
        col("queryCost"),
        col("latitude"),
        col("longitude"),
        col("resolvedAddress"),
        col("timezone"),
        col("tzoffset"),
        col("day.datetime").alias("date"),
        col("day.tempmax"),
        col("day.tempmin"),
        col("day.temp"),
        col("day.humidity"),
        col("day.conditions"),
        col("day.description")
    )

    tmp_folder = f"s3://{AWS_BUCKET_NAME_OUTPUT}/tmp_{today_str}/"
    flat_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_folder)

    tmp_prefix = f"regions/data-output/tmp_{today_str}/"
    objs = list(bucket_output.objects.filter(Prefix=tmp_prefix))
    for obj in objs:
        if obj.key.endswith(".csv"):
            copy_source = {'Bucket': bucket_name_output, 'Key': obj.key}
            final_key = f"regions/data-output/regions_{today_str}.csv"
            bucket_output.copy(copy_source, final_key)
            break

    bucket_output.objects.filter(Prefix=tmp_prefix).delete()

transform_data()
job.commit()
