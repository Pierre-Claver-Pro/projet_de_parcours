import sys
sys.path.insert(0, '/tmp/pylibs')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType
import joblib
import pandas as pd
import numpy as np

# Charger le modèle
loaded_model = joblib.load("/tmp/detector_v1.pkl")

schema = StructType() \
    .add("machine_id", StringType()) \
    .add("valeur", FloatType()) \
    .add("timestamp", StringType()) \
    .add("type_capteur", StringType())

spark = SparkSession.builder \
    .appName("raffinerie-prediction") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "sensor-data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

json_df = json_df.withColumn("timestamp", to_timestamp("timestamp"))

filtrees = json_df.filter(
    ((col("type_capteur") == "temperature") & (col("valeur").between(30, 150))) |
    ((col("type_capteur") == "vibration") & (col("valeur").between(0, 5)))
)

def predict_and_route(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    from pyspark.sql.types import StringType as ST
    batch_df2 = batch_df.withColumn("timestamp", batch_df["timestamp"].cast(ST()))
    pandas_df = batch_df2.toPandas()
    pandas_df['timestamp'] = pd.to_datetime(pandas_df['timestamp'])
    pandas_df = pandas_df.sort_values('timestamp').reset_index(drop=True)
    pandas_df['valeur_lag1'] = pandas_df['valeur'].shift(1)
    pandas_df['valeur_lag2'] = pandas_df['valeur'].shift(2)
    pandas_df['valeur_lag3'] = pandas_df['valeur'].shift(3)
    pandas_df = pandas_df.dropna()
    if pandas_df.empty:
        return
    X = pandas_df[['valeur', 'valeur_lag1', 'valeur_lag2', 'valeur_lag3']]
    predictions = loaded_model.predict(X)
    pandas_df['prediction_score'] = predictions.astype(float)
    anomalies = pandas_df[predictions == -1][['timestamp', 'machine_id', 'type_capteur', 'valeur', 'prediction_score']]
    if not anomalies.empty:
        spark.createDataFrame(anomalies).write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://timescaledb:5432/iotdb") \
            .option("dbtable", "alertes_predictions") \
            .option("user", "admin") \
            .option("password", "admin") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"Batch {batch_id} : {len(anomalies)} anomalie(s) détectée(s) ✅")

query_predict = filtrees.writeStream \
    .foreachBatch(predict_and_route) \
    .option("checkpointLocation", "/app/data/checkpoint_predict") \
    .start()

query_predict.awaitTermination()