import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name, regexp_extract, to_date, dayofmonth, month, year
import pandas as pd
import requests

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

termo_pesquisa = 'data hackers'
TABLE_0 = 'tb_spotify_podcast'
TABLE_1 = 'tb_spotify_unificado'
TABLE_2 = 'tb_spotify_filtrado'
OUTPUT_PATH = "s3://raw-lucassc/"


def pesquisar_podcasts_spotify(termo_pesquisa):
    url = 'https://api.spotify.com/v1/search'
    headers = {
        'Authorization': 'Bearer BQBed99vudgQaWswpjJyN6ZeL70WHOcd9kSl7TANQpxsquOIYdOdyKP3wGIfkknxvuWj3lyki3AUTMO40IL684vVA2dC5djw6jripQxjZmReOcL2h0l3-pxSxaZyYXB2rDw9aPMnPAybzhko9ajmRZZOn0_jTNTHE6HQ5p6V1uU2M2WPQWesSqXEkPikyJBByMt6YfL4dxPmVodFCM6vagE4OStJBunh8FqRwT1kH_qDbMBXsdHJ8OH1S_s0pJDeL-Wbw0tYIlIikUZGjrpH7kcO',
    }
    params = {
        'q': termo_pesquisa,
        'type': 'show',
        'limit': 50
    }

    response = requests.get(url, headers=headers, params=params)
    dados = response.json()

    podcasts = []
    for item in dados['shows']['items']:
        podcast = {
            'name': item['name'],
            'description': item['description'],
            'id': item['id'],
            'total_episodes': item['total_episodes']
        }
        podcasts.append(podcast)

    tabela = spark.createDataFrame(podcasts)
    return tabela

def extrair_episodios_podcast_spotify(podcast_id):
    url = f'https://api.spotify.com/v1/shows/1oMIHOXsrLFENAeM743g93/episodes'
    headers = {
        'Authorization': 'Bearer BQBed99vudgQaWswpjJyN6ZeL70WHOcd9kSl7TANQpxsquOIYdOdyKP3wGIfkknxvuWj3lyki3AUTMO40IL684vVA2dC5djw6jripQxjZmReOcL2h0l3-pxSxaZyYXB2rDw9aPMnPAybzhko9ajmRZZOn0_jTNTHE6HQ5p6V1uU2M2WPQWesSqXEkPikyJBByMt6YfL4dxPmVodFCM6vagE4OStJBunh8FqRwT1kH_qDbMBXsdHJ8OH1S_s0pJDeL-Wbw0tYIlIikUZGjrpH7kcO',
    }

    response = requests.get(url, headers=headers)
    dados = response.json()

    episodios = []
    for item in dados['items']:
        episodio = {
            'id': item['id'],
            'name': item['name'],
            'description': item['description'],
            'release_date': item['release_date'],
            'duration_ms': item['duration_ms'],
            'language': item['language'],
            'explicit': item['explicit'],
            'type': item['type']
        }
        episodios.append(episodio)

    tabela = spark.createDataFrame(episodios)
    return tabela

# Pesquisar os podcasts que possui "Data Hackers" e criar a tabela 5
df = pesquisar_podcasts_spotify(termo_pesquisa)

df_sql = df.createOrReplaceTempView("tb_podcast_spotify")

# tabela 5
df_sql_tb5 = spark.sql("SELECT name, description, id, cast(total_episodes as int)  FROM tb_podcast_spotify")
df_sql_tb5.write.mode('append').parquet(OUTPUT_PATH+TABLE_0+'/')

# Extrair todos os episódios do podcast "Data Hackers" e cra as tabelas 6 e 7
podcast_id = '1oMIHOXsrLFENAeM743g93'  # Substitua pelo ID do podcast "Data Hackers"
df_tabela_episodios = extrair_episodios_podcast_spotify(podcast_id)

df_tabela_episodios.createOrReplaceTempView("tb_podcast_spotify_unificado")

# tbela 6
df_sql_tb6 = spark.sql("SELECT description, cast(duration_ms as int), cast(explicit as string), id, language, name, release_date, type FROM tb_podcast_spotify_unificado")
df_sql_tb6.write.mode('append').parquet(OUTPUT_PATH+TABLE_1+'/')
# tabela 7
df_sql_tb7 = spark.sql("SELECT description, cast(duration_ms as int), cast(explicit as string), id, language, name, release_date, type FROM tb_podcast_spotify_unificado WHERE lower(name) LIKE '%grupo botic_rio%' OR lower(description) LIKE '%grupo botic_rio%'")
df_sql_tb7.write.mode('append').parquet(OUTPUT_PATH+TABLE_2+'/')

print('Schemas')
df_sql_tb5.show(5,truncate=False)
df_sql_tb5.printSchema()
df_sql_tb6.show(5,truncate=False)
df_sql_tb6.printSchema()
df_sql_tb7.show(5,truncate=False)
df_sql_tb7.printSchema()

job.commit()