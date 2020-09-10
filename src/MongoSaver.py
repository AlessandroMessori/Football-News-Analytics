import pandas as pd
from datetime import date


class MongoSaver:

    def __init__(self, con):
        self.counters_df = None
        self.topics = None
        self.con = con

    def load_topics(self):
        self.topics = list(self.con.Topics.find({"category": "player"}).limit(1500))

        for team in self.con.Topics.find({"category": "team"}):
            self.topics.append(team)

    def load_counters(self, language):
        today = date.today()
        (year, month, day) = str(today).split("-")
        self.counters_df = pd.read_csv(
            "https://football-counters.s3.amazonaws.com/" + language + "/" + year + "/" + month + "/" + day + ".csv")

    def save(self, language):
        counters = list()
        for index, row in self.counters_df.iterrows():
            for topic in self.topics:
                if topic["name"] == row["word"]:
                    count = dict()
                    count["name"] = row["word"]
                    count["category"] = topic["category"]
                    count["count"] = row["count"]
                    count["language"] = language
                    count["date"] = str(date.today())
                    counters.append(count)

        self.con.Counters.insert(counters)
