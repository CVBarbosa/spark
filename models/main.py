from pyspark.sql import SparkSession
import datetime
import time
import os
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

print("========= INICIO DO PROCESSO =========")

SPARKMASTER_IP = 'spark://172.20.0.4:7077'

spark = SparkSession.builder.master('local').appName('teste-Mercado')\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    .config("spark.driver.bindAddress", "localhost")\
    .getOrCreate()

print("========= CONEXÃO ABERTA =========")

schema = StructType([
    StructField("NR_ANO", IntegerType(), True), \
    StructField("NR_MES", IntegerType(), True),  \
    StructField("CD_EST_TME", IntegerType(), True), \
    # StructField("DT_VDA", IntegerType, True)
    StructField("CD_EST_FAB", StringType(), True), \
    StructField("CD_EST_DIS", StringType(), True), \
    StructField("CD_EST_PRD", StringType(), True), \
    StructField("CD_IDE_PDV", StringType(), True), \
    StructField("NUM_CD_IDE_PDV", StringType(), True), \
    StructField("QT_VDA", DoubleType(), True), \
    StructField("VL_VDA", DoubleType(), True), \
    StructField("TP_TRA_MOV", StringType(), True), \
    StructField("IC_MOVINC", StringType(), True), \
]
)

df = spark.read.format("csv").option("sep",";").option("header", "true").schema(schema).load("/data/mercadoDM_BARUEL_20211101.csv")

df.printSchema()

df.show()


# df.show(10, truncate = True)

# b = 1
# while(True):
#     print('passagem: {}'.format(b))
#     b += 1

#     time.sleep(3)

# df.show()

# a = 1
# b = 1
# while a == 1:
#     print('passagem: {}'.format(b))
#     b += 1

#     time.sleep(3)

# from pyspark.sql import SparkSession

# SPARKMASTER_IP = 'spark://master:7077'

# spark = SparkSession.builder.master(SPARKMASTER_IP).appName("my app").enableHiveSupport().getOrCreate()

# print("Sessao conf.")

# print(12*'=')
# print("[LOG] - SESSÃO CONFIGURADA")
# print("[LOG] - AGUARDANDO MENSAGEM")
# print(12*'=')

# for receiver_message in kfk.get_consumer():
#     print(12*'=')
#     print("[LOG] - MENSAGEM RECEBIDA")
#     print(12*'=')
#     try:
#         dataframe = Dataframe(spark)
#         mensagem = Mensagem(mensagem=receiver_message)
#         hudi = HudiMapper(spark)
#         minio = FileMinIO()
#         log_sessao = []; particoes = []
#         print("[PASSO 0] - Mensagem recebida. Offset id: {}".format(mensagem.get_full_message().offset))

#         log_sessao.append("mensagem recebida")

#         data_inicio_proc = datetime.datetime.utcnow()
#         inicio_cronometro = time.time()

#         print("[PASSO 1] - Carregando e tipando dataset")
#         df = dataframe.get_dataframe(
#             path_file=mensagem.get_path_file(),
#             format=mensagem.get_type_file()
#         )

#         # A função acima sempre vai me retornar um dataframe
#         print("[PASSO 2] - Gravando no HUDI")

#         # Adicionar o save via MinIO
#         hudi.write_dataset(
#             df=df,
#             path=os.environ['BUCKET_HUDI'],
#             table="vendas_arquivos_processados",
#             recordkey=dataframe.get_record_keys('venda'),
#             partition_field=dataframe.get_partitions('venda'),
#             precombine_field="DT_VENDA"
#         )
#         # Construíndo mensagem para próximo tópico
#         publish_message = {
#             'path_file': mensagem.get_values_message()['path_arquivo'],
#             'log_sessao': log_sessao,
#             'id_arquivo': mensagem.get_values_message()['path_arquivo'].split('-')[0],
#             'partitions': particoes,
#             'dt_proc_start': str(data_inicio_proc),
#             'dt_proc_end': str(datetime.datetime.utcnow()),
#             'topico': mensagem.get_full_message().topic,
#             'status_proc': 'success',
#             'offset_id': mensagem.get_full_message().offset,
#             'type_proc': 'full',
#             'company': 'como_pegar?',
#             'total_time_proc': str(time.time() - inicio_cronometro)
#         }

#         print("[PASSO 3] - Publicando mensagem")
#         mensagem.publisher(topico=os.environ['KAFKA_PRODUCER_SUCCESS_TOPIC'], message=publish_message)
#     except Exception as e:
#         print(str(e))
#         print("[ERRO] - Erro ao processar mensagem")
#         publish_message = {
#             'path_file': mensagem.get_values_message()['path_arquivo'],
#             'log_sessao': log_sessao,
#             'id_arquivo': mensagem.get_values_message()['path_arquivo'].split('-')[0],
#             'partitions': particoes,
#             'dt_proc_start': str(data_inicio_proc),
#             'dt_proc_end': str(datetime.datetime.utcnow()),
#             'topico': mensagem.get_full_message().topic,
#             'status_proc': 'failure',
#             'offset_id': mensagem.get_full_message().offset,
#             'type_proc': 'full',
#             'company': 'como_pegar?',
#             'total_time_proc': str(time.time() - inicio_cronometro)
#         }
#         mensagem.publisher(topico=os.environ['KAFKA_PRODUCER_FAILURE_TOPIC'], message=publish_message) # https://stackoverflow.com/questions/50214940/how-to-set-acknowledgement-to-false-in-kafka
