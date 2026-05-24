from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType
import joblib
import pandas as pd

loaded_model = joblib.load('/app/models/detector_v1.pkl')
print('Modele charge OK')

schema = StructType() \
    .add('machine_id', StringType()) \
    .add('valeur', FloatType()) \
    .add('timestamp', StringType()) \
    .add('type_capteur', StringType())

spark = SparkSession.builder.appName('raffinerie-prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:29092') \
    .option('subscribe', 'sensor-data') \
    .option('startingOffsets', 'latest') \
    .option('failOnDataLoss', 'false') \
    .load()

json_df = df.selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), schema).alias('data')) \
    .select('data.*')

json_df = json_df.withColumn('timestamp', to_timestamp('timestamp'))

def predict_and_route(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        return
    pdf = batch_df.withColumn("timestamp", batch_df["timestamp"].cast("string")).toPandas()
    pdf = pdf.dropna(subset=['valeur'])
    if pdf.empty:
        return
    pdf['timestamp'] = pd.to_datetime(pdf['timestamp'])
    predictions = loaded_model.predict(pdf[['valeur']])
    scores = loaded_model.decision_function(pdf[['valeur']])
    pdf['prediction_score'] = scores
    def get_severite(score):
        if score < -0.15: return 'CRITIQUE'
        elif score < -0.05: return 'WARNING'
        else: return 'NORMAL'
    pdf['severite'] = pdf['prediction_score'].apply(get_severite)
    anomalies = pdf[predictions == -1][['timestamp','machine_id','type_capteur','valeur','prediction_score','severite']].copy()
    anomalies['timestamp'] = anomalies['timestamp'].astype(str)
    print(f'Batch {batch_id}: {count} lignes, {len(anomalies)} anomalies')
    if not anomalies.empty:
        spark.createDataFrame(anomalies).write \
            .format('jdbc') \
            .option('url', 'jdbc:postgresql://timescaledb:5432/iotdb') \
            .option('dbtable', 'alertes_predictions') \
            .option('user', 'admin') \
            .option('password', 'admin') \
            .option('driver', 'org.postgresql.Driver') \
            .mode('append') \
            .save()
        print(f'{len(anomalies)} anomalies ecrites!')

query_predict = json_df.writeStream \
    .foreachBatch(predict_and_route) \
    .option('checkpointLocation', '/app/data/checkpoint_predict') \
    .start()

query_predict.awaitTermination()
