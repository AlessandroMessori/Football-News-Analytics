import pymongo
import json
from src.MongoSaver import MongoSaver

with open('/usr/local/airflow/config.json', 'r') as f:
    config = json.load(f)
    languages = config["languages"]

    with open('/usr/local/airflow/cred.json', 'r') as f2:
        cred = json.load(f2)
        client = pymongo.MongoClient(
            'mongodb://%s:%s@3.215.77.214/football-dashboard' % (cred["username"], cred["password"]))
        db = client["football-dashboard"]
        mongoSaver = MongoSaver(db)
        mongoSaver.load_topics()

        for lan in languages:
            mongoSaver.load_counters(lan["id"])
            mongoSaver.save(lan["id"])
