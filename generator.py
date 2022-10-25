from configparser import ConfigParser
from datetime import datetime
from random import choice

from kafka import KafkaProducer
from numpy.random import normal


def main():
    PROPERTIES_FILE = "generator.properties"
    CLIENT_FILE = "clients.txt"

    config_reader = ConfigParser()
    config_reader.read(PROPERTIES_FILE)
    config = {
        "clients-amount": int(config_reader.get("DEFAULT", "clients-amount")),
        "N": int(config_reader.get("DEFAULT", "messages-amount")),
        "amount.mean": float(config_reader.get("amount", "mean")),
        "amount.std": float(config_reader.get("amount", "std")),
        "kafka.bootstrap-server": config_reader.get("kafka", "bootstrap-server"),
        "kafka.topic": config_reader.get("kafka", "topic"),
    }

    clients = []
    with open(CLIENT_FILE, "r") as file:
        clients = file.read().splitlines()

    producer = KafkaProducer(bootstrap_servers=[config["kafka.bootstrap-server"]])

    start_time = datetime.now()
    print("-" * 40)
    print("started at ", start_time)
    print("-" * 40)

    for i in range(1, config["N"] + 1):
        producer.send(
            config["kafka.topic"],
            value=bytearray(
                f"{i},"
                + f"{choice(clients)},abc,"
                + f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')},"
                + f"{normal(config['amount.mean'], config['amount.std'])}",
                "utf-8",
            ),
        )

    end_time = datetime.now()
    print("ended at ", end_time)
    print("-" * 40)
    print(f"Sending {config['N']} messages took {end_time - start_time}")


if __name__ == "__main__":
    main()
