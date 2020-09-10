import pymongo
import json
from src.FifaDBExport import FifaDBExport

with open('/usr/local/airflow/cred.json', 'r') as f:
    config = json.load(f)
    client = pymongo.MongoClient(
        'mongodb://%s:%s@3.215.77.214/football-dashboard' % (config["username"], config["password"]))
    db = client["football-dashboard"]

    exporter = FifaDBExport("../../Fifa20/players_20.csv", "../../Fifa20/teams_and_leagues.csv", db)

    exporter.load_teams_to_df()

    print(exporter.teams_df.head(100))

    # exporter.export_to_mongoDB("player")
