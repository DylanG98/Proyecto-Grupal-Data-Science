import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from awsglue.dynamicframe import DynamicFrame


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

order_df = glueContext.create_dynamic_frame_from_options(connection_type= 's3',
                                                    connection_options={"paths": [input_file_path]},
                                                    format='csv', format_options = {"withHeader": True})

class MyGlueJob:
    def __init__(self, order_df):
        
        self.order_df = order_df
        
    def run_job(self):

        '''
        ETL para la metadata
        '''

        # Definimos el mapping que aplicará la transformación a las columnas. 
        mapping = [("name", "string", "name", "string"),
                   ("address", "string", "address", "string"),
                   ("gmap_id", "string", "gmap_id", "string"),
                   ("latitude", "string", "latitude", "float"),
                   ("longitude", "string", "longitude", "float"),
                   ("category", "string", "category", "string"),
                   ("num_of_reviews", "string", "num_of_reviews", "int"),
                   ("avg_rating", "string", "avg_rating", "float"),
                   ("MISC", "string", "MISC", "string")]
        
        # Aplico el mapping al df dynmic
        order_df_transformed = ApplyMapping.apply(frame=self.order_df,
                                                   mappings=mapping,
                                                   transformation_ctx="order_df_transformed")

        # Transformo en df spark para realizarle el ETL
        df = order_df_transformed.toDF()

        df2 = df.select("*", from_json("MISC", "map<string,string>").alias("mapa_misc"))

        # Usar explode para crear una fila por cada clave del diccionario
        df3 = df2.select("*", explode("mapa_misc").alias("clave", "valor"))

        # Cambio
        df3 = df3.withColumn("clave", regexp_replace("clave", " ", "_"))

        # Usar pivot para convertir las claves en columnas
        df4 = df3.groupBy("gmap_id").pivot("clave").agg({"valor": "first"})

        # Unir el resultado de pivot con las columnas originales
        df_final = df3.join(df4, on="gmap_id")

        # Selecciono columnas que me interesan
        df = df_final.select('gmap_id', 'name', 'address', 'latitude', 'longitude', 'num_of_reviews', 'avg_rating', 'category', 'Dining_options', 'Service_options', 'Payments')

        # Las renombro para estandarizarlas
        df = df \
            .select(col("gmap_id").alias("gmap_id"), \
                    col("name").alias("name"), \
                    col("address").alias("address"), \
                    col("latitude").alias("latitude"), \
                    col("longitude").alias("longitude"), \
                    col("num_of_reviews").alias("num_reviews"), \
                    col("avg_rating").alias("avg_rating"), \
                    col("category").alias("category"), \
                    col("Dining_options").alias("dining_options"), \
                    col("Service_options").alias("service_options")) \
            .dropDuplicates()

        # Agrego columnas de mi interés a partir de data en columnas extraídas
        df = df \
            .withColumn("delivery", when(lower(col("service_options")).contains("delivery"), "yes").otherwise("no")) \
            .withColumn("takeout", when(lower(col("service_options")).contains("takeout"), "yes").otherwise("no"))  \
            .withColumn("serves_breakfast", when(lower(col("dining_options")).contains("breakfast"), "yes").otherwise("no")) \
            .withColumn("serves_lunch", when(lower(col("dining_options")).contains("lunch"), "yes").otherwise("no")) \
            .withColumn("serves_dinner", when(lower(col("dining_options")).contains("dinner"), "yes").otherwise("no"))
            
        # Dropeo columnas innecesarias
        df = df.drop("service_options")
        df = df.drop("dining_options")
        
        # Escribo el df sobre un bucket en s3
        df.coalesce(1).write.format("parquet").option("header", "true").save("s3://stagedata-bucket00/{}".format(file_name.split('.')[0]))
        job.commit()



job_metadata = MyGlueJob(order_df)
job_metadata.run_job()