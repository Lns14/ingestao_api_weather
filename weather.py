import requests
import json
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType, FloatType
from pyspark.sql.functions import explode
from pyspark.sql import Row
from pyspark.sql.functions import lit

# Inicializa a Spark session
spark = SparkSession.builder \
    .appName("API de tempo") \
    .getOrCreate()

# Variáveis de entrada
cities = ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Belo Horizonte"]

# Bloco de consulta a API
def get_weather_data(city):
    try:
        key = "ddf8227c387ad9d0d19ba9437d284b09"
        params = {
            "q": city,
            "appid": key,
            "units": "metric",  
            "lang": "pt_br"
        }
        URL = f"https://api.openweathermap.org/data/2.5/forecast?"
        response = requests.get(URL, params=params)
        data = ""
        message = ""

        # Verifica se a requisição foi bem-sucedida
        if response.status_code == 200:
            data = response.json()

            # Cria um DataFrame a partir dos dados retornados pela API
            weather = StructType([
                StructField("city", StringType(), True),
                StructField("dt_txt", StringType(), True),
                StructField("main", StructType([
                    StructField("temp", StringType(), True),
                    StructField("feels_like", StringType(), True),
                    StructField("temp_min", StringType(), True),
                    StructField("temp_max", StringType(), True),
                    StructField("pressure", IntegerType(), True),
                    StructField("humidity", IntegerType(), True)
                ]), True),
                StructField("weather", ArrayType(
                    StructType([
                        StructField("main", StringType(), True),  
                        StructField("description", StringType(), True),
                    ])
                ), True)
            ])  
            
            df = spark.createDataFrame(data["list"], schema=weather)
            df = df.withColumn("city", F.lit(city)) 
            df = df.withColumn("dt_txt", F.to_timestamp(F.col("dt_txt")))
            df = df.withColumn("weather", explode(F.col("weather")))
            df = df.select(
                F.col("city"),
                F.col("dt_txt"),
                F.col("main.temp").alias("temp"),
                F.col("main.feels_like").alias("feels_like"),
                F.col("main.temp_min").alias("temp_min"),
                F.col("main.temp_max").alias("temp_max"),
                F.col("main.pressure").alias("pressure"),
                F.col("main.humidity").alias("humidity"),
                F.col("weather.description").alias("weather_description")
            ) 

            return df


        # Se a requisição falhar, imprime o erro
        elif response.status_code == 404:
            message = "Erro no endponint ou recurso não encontrado."
        elif response. status_code == 401:
            message = "Erro de autenticação. Verifique a chave de API."
        elif response.status_code == 400:
            message = "Erro de requisição. Verifique os parâmetros passados."
        elif response.status_code == 403:
            message = "Sua conta não tem permissão para acessar esse recurso."
        elif response.status_code == 429:
            message = "Limite de requisições atingido. Tente novamente mais tarde."
        elif response.status_code == 500:
            message = "Erro interno do servidor. Tente novamente mais tarde."

    except Exception as e:
        print("Erro inesperado:", e)

df_total = None

for city in cities:
    print(f"Consultando dados de {city}...")
    df_city = get_weather_data(city)
    if df_city is not None:
        if df_total is None:
            df_total = df_city
        else:
            df_total = df_total.union(df_city)

# Salva se tiver dados
if df_total is not None:
    df_total.write.mode("overwrite").parquet("/mnt/datalake/bronze/weather_parquet")
    print("Dados salvos com sucesso.")
    df_total.show()
else:
    print("Nenhum dado foi retornado pelas consultas.")
