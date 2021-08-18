import json


def backup_subcribers(channels: dict):
    with open("subcribers.json", "w") as f:
        json.dump(channels, f, indent=4)
