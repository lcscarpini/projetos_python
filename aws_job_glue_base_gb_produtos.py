import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name, regexp_extract, to_date, dayofmonth, month, year

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

PATH = "s3://myawsbucket-lucassc/csv/"
TABLE_0 = 'tb_gb_produtos'
TABLE_1 = 'tb_consolidado_ano_mes'
TABLE_2 = 'tb_consolidado_marca_linha'
TABLE_3 = 'tb_consolidado_marca_ano_mes'
TABLE_4 = 'tb_consolidado_linha_ano_mes'
OUTPUT_PATH = "s3://raw-lucassc/"

df = spark.read.csv(PATH, sep=';', header=True, inferSchema=True)\
    .withColumn("nome_arquivo", regexp_extract(input_file_name(), "[^/]*$", 0))

df.write.mode('append').parquet(OUTPUT_PATH+TABLE_0+'/')

df0 = df.withColumn("data", to_date(df["DATA_VENDA"], "M/d/yy"))

# Cria as colunas separadas para dia, mês e ano
df0 = df0.withColumn("dia", dayofmonth(df0["data"]))
df0 = df0.withColumn("mes", month(df0["data"]))
df0 = df0.withColumn("ano", year(df0["data"]))

df0 = df0.drop("DATA_VENDA","data","nome_arquivo")

df0.createOrReplaceTempView("tb_vendas_marca_linha")

# Tabela 1: Consolidado de vendas por ano e mês;
# tb_consolidado_ano_mes
df1 = spark.sql("SELECT ano, mes, cast(count(QTD_VENDA) as int) as qtd_venda  FROM tb_vendas_marca_linha GROUP BY ano, mes ORDER BY ano, mes")
df1.write.mode('append').parquet(OUTPUT_PATH+TABLE_1+'/')

# Tabela 2: Consolidado de vendas por marca e linha;
# tb_consolidado_marca_linha
df2 = spark.sql("SELECT MARCA, LINHA, cast(count(QTD_VENDA) as int) as qtd_venda FROM tb_vendas_marca_linha GROUP BY MARCA, LINHA ORDER BY 1, 2")
df2.write.mode('append').parquet(OUTPUT_PATH+TABLE_2+'/')

# Tabela 3: Consolidado de vendas por marca, ano e mês;
# tb_consolidado_marca_ano_mes
df3 = spark.sql("SELECT MARCA, ano, mes, cast(count(QTD_VENDA) as int) as qtd_venda FROM tb_vendas_marca_linha GROUP BY MARCA, ano, mes ORDER BY 1, 2, 3")
df3.write.mode('append').parquet(OUTPUT_PATH+TABLE_3+'/')

# Tabela 4: Consolidado de vendas por linha, ano e mês;
# tb_consolidado_linha_ano_mes
df4 = spark.sql("SELECT LINHA, ano, mes, cast(count(QTD_VENDA) as int) as qtd_venda FROM tb_vendas_marca_linha GROUP BY LINHA, ano, mes ORDER BY 1, 2, 3")
df4.write.mode('append').parquet(OUTPUT_PATH+TABLE_4+'/')

job.commit()