import boto3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configuração da AWS S3
AWS_ACCESS_KEY_ID = 'YOUR_AWS_ACCESS_KEY_ID'
AWS_SECRET_ACCESS_KEY = 'YOUR_AWS_SECRET_ACCESS_KEY'
BUCKET_NAME = 'YOUR_BUCKET_NAME'
OBJECT_KEY = 'YOUR_OBJECT_KEY'
LOCAL_TEMP_FILE = '/tmp/temp_file.parquet'

# Cria uma sessão PySpark
spark = SparkSession.builder.appName('S3 Download and Processing').getOrCreate()

# Faz download do arquivo Parquet do S3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3.download_file(Bucket=BUCKET_NAME, Key=OBJECT_KEY, Filename=LOCAL_TEMP_FILE)

# Lê o arquivo Parquet usando PySpark
df = spark.read.parquet(LOCAL_TEMP_FILE)

# Aplica filtros e seleciona colunas específicas
filtered_df = (
    df.filter(col('column_name') == 'filter_value')  
    .select('column1', 'column2', 'column3')
)

# Armazena os dados em cache
cached_df = filtered_df.cache()

# Mostra os dados processados
cached_df.show()

# Remove o arquivo temporário local
os.remove(LOCAL_TEMP_FILE)
