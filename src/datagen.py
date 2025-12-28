import time
import uuid
import random
import psycopg2
import json
from datetime import datetime
from faker import Faker

fake = Faker()

DB_PARAMS = {
    "host": "postgres",
    "database": "engagement_db",
    "user": "postgres",
    "password": "postgres"
}

def get_conn():
    while True:
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            return conn
        except psycopg2.OperationalError:
            print("Waiting for Postgres...")
            time.sleep(2)

def create_content(conn, n=50):
    cur = conn.cursor()
    content_ids = []
    print(f"Generating {n} content items...")
    for _ in range(n):
        c_id = str(uuid.uuid4())
        slug = fake.slug()
        title = fake.sentence(nb_words=4)
        c_type = random.choice(['podcast', 'newsletter', 'video'])
        length = random.randint(60, 3600) # Random length between 1 minute and 1 hour
        publish_ts = datetime.now()
        
        cur.execute("""
            INSERT INTO content (id, slug, title, content_type, length_seconds, publish_ts)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (c_id, slug, title, c_type, length, publish_ts))
        content_ids.append(c_id)
    
    conn.commit()
    cur.close()
    return content_ids

def generate_events(conn, content_ids):
    cur = conn.cursor()
    print("Starting event generation stream...")
    while True:
        c_id = random.choice(content_ids)
        u_id = str(uuid.uuid4())
        e_type = random.choice(['play', 'pause', 'finish', 'click'])
        event_ts = datetime.now()
        duration = random.randint(1000, 300000) if e_type in ['play', 'finish'] else None
        device = random.choice(['ios', 'android', 'web-safari', 'web-chrome'])
        payload = json.dumps({"metadata": "test", "ip": fake.ipv4()})
        
        cur.execute("""
            INSERT INTO engagement_events (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (c_id, u_id, e_type, event_ts, duration, device, payload))
        
        conn.commit()
        print(f"Inserted event: {e_type} for content {c_id}")
        time.sleep(random.uniform(0.1, 1.0)) # Sleep a bit to simulate real-time traffic

if __name__ == "__main__":
    conn = get_conn()
    content_ids = create_content(conn)
    generate_events(conn, content_ids)
