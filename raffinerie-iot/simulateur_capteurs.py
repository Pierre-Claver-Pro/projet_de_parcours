import time, json, random
import paho.mqtt.client as mqtt # pyright: ignore[reportMissingImports]
 
client = mqtt.Client()
client.connect("localhost", 1883, 60)
 
cycle = 0
while True:
    now = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    cycle += 1
 
    # Température normale entre 60-90°C, anomalie toutes les 20 mesures
    if cycle % 20 == 0:
        temp = round(random.uniform(130, 150), 2)  # anomalie chaude
    else:
        temp = round(random.uniform(60, 90), 2)    # normale
 
    # Vibration normale entre 0.5-2.5, anomalie toutes les 25 mesures
    if cycle % 25 == 0:
        vib = round(random.uniform(4, 5), 2)       # anomalie forte
    else:
        vib = round(random.uniform(0.5, 2.5), 2)   # normale
 
    msg_temp = json.dumps({
        "machine_id": "pipe-101",
        "valeur": temp,
        "timestamp": now,
        "type_capteur": "temperature"
    })
 
    msg_vib = json.dumps({
        "machine_id": "pump-303",
        "valeur": vib,
        "timestamp": now,
        "type_capteur": "vibration"
    })
 
    client.publish("raffinerie/temp", msg_temp)
    client.publish("raffinerie/vib", msg_vib)
 
    print(f"Publiés → Temp: {temp}°C | Vib: {vib} mm/s {'⚠️ ANOMALIE' if cycle % 20 == 0 or cycle % 25 == 0 else ''}")
    time.sleep(2)