import boto3
import json
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import joblib

# Connexion à MinIO
s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minio',
    aws_secret_access_key='minio123'
)

# Télécharger les fichiers JSON depuis MinIO
print("Téléchargement des données depuis MinIO...")
objects = s3.list_objects_v2(Bucket='raffinerie-raw', Prefix='raw/')
records = []

for obj in objects.get('Contents', []):
    response = s3.get_object(Bucket='raffinerie-raw', Key=obj['Key'])
    content = response['Body'].read().decode('utf-8')
    for line in content.strip().split('\n'):
        if line:
            try:
                records.append(json.loads(line))
            except:
                pass

# Garder uniquement les vraies données capteurs
df = pd.DataFrame(records)
df = df[['machine_id', 'valeur', 'timestamp', 'type_capteur']].dropna()
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['valeur'] = pd.to_numeric(df['valeur'], errors='coerce')
df = df.dropna().sort_values('timestamp').reset_index(drop=True)

print(f"{len(df)} enregistrements valides chargés.")

# Créer les lags par type de capteur
dfs = []
for capteur in df['type_capteur'].unique():
    df_c = df[df['type_capteur'] == capteur].copy().reset_index(drop=True)
    df_c['valeur_lag1'] = df_c['valeur'].shift(1)
    df_c['valeur_lag2'] = df_c['valeur'].shift(2)
    df_c['valeur_lag3'] = df_c['valeur'].shift(3)
    dfs.append(df_c)

df_final = pd.concat(dfs).dropna().reset_index(drop=True)
print(f"{len(df_final)} enregistrements après création des features.")

# Entraînement du modèle
print("Entraînement du modèle...")
X = df_final[['valeur', 'valeur_lag1', 'valeur_lag2', 'valeur_lag3']]
model = IsolationForest(contamination=0.05, random_state=42)
model.fit(X)

# Sauvegarde
joblib.dump(model, '/tmp/detector_v1.pkl')
print("Modèle sauvegardé dans models/detector_v1.pkl ✅")