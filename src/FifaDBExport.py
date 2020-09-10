import pandas as pd


class FifaDBExport:

    def __init__(self, players_path, teams_path, con):
        self.players_path = players_path
        self.teams_path = teams_path
        self.con = con
        self.players_df = None
        self.teams_df = None

    def load_players_to_df(self):
        self.players_df = pd.read_csv(self.players_path)

    def load_teams_to_df(self):
        self.teams_df = pd.read_csv(self.teams_path)

    def export_to_mongoDB(self, category):
        names = list(self.players_df["short_name"]) if (category == "player") else list(self.teams_df["short_name"])
        topics = list()
        for name in names:
            words = name.split(" ")
            print(name)
            surname = (words[-1] if (len(words[-1]) > 2 or len(words) < 3) else words[-2])
            topics.append({"name": surname, "alias": [], "category": category})

        self.con.Topics.insert(topics)
