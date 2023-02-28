import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.functions import expr
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["VAL1","VAL2"])
file_name=args['VAL1']
bucket_name=args['VAL2']
print("Bucket Name" , bucket_name)
print("File Name" , file_name)
input_file_path="s3://{}/{}".format(bucket_name,file_name)
print("Input File Path : ",input_file_path);

df = glueContext.create_dynamic_frame_from_options(connection_type= 's3',
                                                    connection_options={"paths": [input_file_path]},
                                                    format='json', format_options = {"withHeader": True})


class MyGlueJob():
    def __init__(self, df):
        self.df = df

    
    def run_job(self):


        # Definimos el mapping que aplicará la transformación a la columna 'time'
        mapping = [("user_id", "string", "user_id", "float"),
                ("name", "string", "name", "string"),
                ("time", "long", "time", "long"),
                ("rating", "int", "rating", "int"),
                ("text", "string", "text", "string"),
                ("gmap_id", "string", "gmap_id", "string")]

        df = ApplyMapping.apply(frame=self.df,
                                mappings=mapping,
                                transformation_ctx="df")

        # Convertimos DynamicFrame a DataFrame
        df = df.toDF()

        # Convertimos la columna "time" al formato de fecha deseado utilizando select() y expr()
        df = df.select("*", expr("to_date(from_unixtime(time / 1000))").alias("date"))

        # Eliminamos la columna "time" original
        df = df.drop("time")

        df.coalesce(1).write.format("parquet").option("header", "true").save("s3://stagedata-bucket00/{}".format(file_name.split('.')[0]))
        job.commit()


job_metadata = MyGlueJob(df)
job_metadata.run_job()