import json
from src.TopicCounter import TopicCounter

with open('/usr/local/airflow/config.json', 'r') as f:
    config = json.load(f)
    languages = config["languages"]

    for lan in languages:
        topic_counter = TopicCounter("/usr/local/airflow/data/" + lan["name"] + ".csv")
        topic_counter.get_raw_text()
        counters_df = topic_counter.get_counters()
        counters_df.to_csv("/usr/local/airflow/counters/" + lan["name"] + ".csv", index=False)
