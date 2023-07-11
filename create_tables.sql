-- TABELA 0
-- tb_gb_produtos

CREATE EXTERNAL TABLE IF NOT EXISTS `data_base_case_1`.`tb_gb_produtos` (
  `id_marca` int,
  `marca` string,
  `id_linha` int,
  `linha` string,
  `data_venda` timestamp,
  `qtd_venda` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://raw-lucassc/tb_gb_produtos'
TBLPROPERTIES ('classification' = 'parquet');

--DROP TABLE tb_gb_produtos

-- TABELA 1
-- tb_consolidado_ano_mes

CREATE EXTERNAL TABLE IF NOT EXISTS `data_base_case_1`.`tb_consolidado_ano_mes` (
  `ano` int,
  `mes` int,
  `qtd_venda` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://raw-lucassc/tb_consolidado_ano_mes'
TBLPROPERTIES ('classification' = 'parquet');

-- TABELA 2
-- tb_consolidado_marca_linha

CREATE EXTERNAL TABLE IF NOT EXISTS `data_base_case_1`.`tb_consolidado_marca_linha` (
  `marca` string,
  `linha` string,
  `qtd_venda` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://raw-lucassc/tb_consolidado_marca_linha'
TBLPROPERTIES ('classification' = 'parquet');

-- TABELA 3
-- tb_consolidado_marca_ano_mes

CREATE EXTERNAL TABLE IF NOT EXISTS `data_base_case_1`.`tb_consolidado_marca_ano_mes` (
  `marca` string,
  `ano` int,
  `mes` int,
  `qtd_venda` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://raw-lucassc/tb_consolidado_marca_ano_mes'
TBLPROPERTIES ('classification' = 'parquet');

-- TABELA 4
-- tb_consolidado_linha_ano_mes

CREATE EXTERNAL TABLE IF NOT EXISTS `data_base_case_1`.`tb_consolidado_linha_ano_mes` (
  `linha` string,
  `ano` int,
  `mes` int,
  `qtd_venda` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://raw-lucassc/tb_consolidado_linha_ano_mes'
TBLPROPERTIES ('classification' = 'parquet');




-- TABELA 5
-- tb_spotify_podcast

CREATE EXTERNAL TABLE IF NOT EXISTS `data_base_case_2`.`tb_spotify_podcast` (
 `name` string, 
 `description` string,
 `id` string,
 `total_episodes` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://raw-lucassc/tb_spotify_podcast'
TBLPROPERTIES ('classification' = 'parquet');

-- TABELA 6
-- tb_spotify_unificado

CREATE EXTERNAL TABLE IF NOT EXISTS `data_base_case_2`.`tb_spotify_unificado` (
 `description` string,
 `duration_ms` int, 
 `explicit` string,
 `id` string,
 `language` string,
 `name` string,
 `release_date` string,
 `type` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://raw-lucassc/tb_spotify_unificado'
TBLPROPERTIES ('classification' = 'parquet');

-- TABELA 7
-- tb_spotify_filtrado

CREATE EXTERNAL TABLE IF NOT EXISTS `data_base_case_2`.`tb_spotify_filtrado` (
 `description` string,
 `duration_ms` int, 
 `explicit` string,
 `id` string,
 `language` string,
 `name` string,
 `release_date` string,
 `type` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://raw-lucassc/tb_spotify_filtrado'
TBLPROPERTIES ('classification' = 'parquet');