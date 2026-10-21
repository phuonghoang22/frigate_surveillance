import json
import psycopg2
from psycopg2.extras import Json
import paho.mqtt.client as mqtt
from datetime import datetime

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "frigate_db",
    "user": "admin",
    "password": "password123"
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("[+] Ket noi MQTT Broker thanh cong!")
        client.subscribe("frigate/events")
        print("[+] Dang lang nghe topic: frigate/events")
    else:
        print(f"[-] Loi ket noi MQTT, code: {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        data = payload.get("after", {})
        
        event_id = data.get("id")
        camera = data.get("camera")
        label = data.get("label")
        top_score = data.get("top_score")
        start_time = datetime.fromtimestamp(data.get("start_time")) if data.get("start_time") else None
        end_time = datetime.fromtimestamp(data.get("end_time")) if data.get("end_time") else None

        if not event_id:
            return

        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
        INSERT INTO frigate_events (id, camera, label, top_score, start_time, end_time, raw_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            top_score = EXCLUDED.top_score,
            end_time = EXCLUDED.end_time,
            raw_message = EXCLUDED.raw_message;
        """
        cursor.execute(query, (event_id, camera, label, top_score, start_time, end_time, Json(payload)))
        conn.commit()
        cursor.close()
        conn.close()

        print(f"[EVENT] Da luu su kien '{label}' (Score: {top_score}) tu camera '{camera}' vao PostgreSQL")
    except Exception as e:
        print(f"[-] Loi xu ly message: {e}")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message

print("[*] Dang ket noi toi MQTT Broker...")
client.connect("127.0.0.1", 1883, 60)
client.loop_forever()
