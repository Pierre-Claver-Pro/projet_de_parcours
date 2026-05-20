import paho.mqtt.client as mqtt # pyright: ignore[reportMissingImports]
from kafka import KafkaProducer, KafkaAdminClient # pyright: ignore[reportMissingImports]
from kafka.admin import NewTopic # pyright: ignore[reportMissingImports]
import json

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "sensor-data"

try:
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)]
    admin_client.create_topics(new_topics=topic_list)
except Exception as e:
    print(f"Erreur ou topic déjà existant : {e}")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        producer.send(TOPIC_NAME, data)
        producer.flush()
    except Exception as e:
        print(f"Erreur lors de la publication vers Kafka : {e}")

client = mqtt.Client()
client.connect("localhost", 1883)
client.subscribe("raffinerie/temp")
client.subscribe("raffinerie/vib")
client.on_message = on_message
client.loop_forever()
