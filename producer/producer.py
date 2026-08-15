import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "transactions")
EVENTS_PER_SEC = float(os.environ.get("EVENTS_PER_SEC", "500"))
NUM_USERS = int(os.environ.get("NUM_USERS", "200"))
ANOMALY_INTERVAL_SEC = float(os.environ.get("ANOMALY_INTERVAL_SEC", "30"))

LOCATIONS = ["NYC", "SF", "LON", "SGP", "BLR", "SYD", "TOR", "BER"]


class UserProfile:
    __slots__ = ("user_id", "mu", "sigma", "device_id", "home_location")

    def __init__(self, idx: int):
        self.user_id = f"user_{idx:04d}"
        self.mu = random.uniform(2.3, 4.0)
        self.sigma = random.uniform(0.3, 0.6)
        self.device_id = f"dev_{uuid.uuid4().hex[:8]}"
        self.home_location = random.choice(LOCATIONS)

    def normal_amount(self) -> float:
        return round(random.lognormvariate(self.mu, self.sigma), 2)

    def anomalous_amount(self) -> float:
        baseline = round(pow(2.718281828, self.mu), 2)
        return round(baseline * random.uniform(40, 120), 2)


def build_event(profile: UserProfile, anomalous: bool = False) -> dict:
    amount = profile.anomalous_amount() if anomalous else profile.normal_amount()
    return {
        "user_id": profile.user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": amount,
        "location": profile.home_location if not anomalous else random.choice(LOCATIONS),
        "device_id": profile.device_id,
        "injected_anomaly": anomalous,
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=10,
        acks="all",
    )

    profiles = [UserProfile(i) for i in range(NUM_USERS)]
    delay = 1.0 / EVENTS_PER_SEC
    last_anomaly_ts = time.time()

    print(f"Producing to topic '{TOPIC}' at ~{EVENTS_PER_SEC} events/sec with {NUM_USERS} simulated users...")

    sent = 0
    while True:
        profile = random.choice(profiles)

        inject = (time.time() - last_anomaly_ts) >= ANOMALY_INTERVAL_SEC
        if inject:
            last_anomaly_ts = time.time()

        event = build_event(profile, anomalous=inject)
        producer.send(TOPIC, key=profile.user_id, value=event)
        sent += 1

        if sent % 1000 == 0:
            producer.flush()
            print(f"sent={sent}")

        time.sleep(delay)


if __name__ == "__main__":
    main()
