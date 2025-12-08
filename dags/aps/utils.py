import json
from datetime import date, datetime
from io import BytesIO


def save_file_in_s3(data, repo):
    if not bool(data):
        return
    date_today = date.today().strftime("%Y-%m-%d")
    prefix = datetime.now().strftime("%Y-%m-%dT%H:%M")

    byte_file = BytesIO(data)
    key = f"{date_today}/{prefix}.json"
    repo.save(key, byte_file)
    return key


def split_json(repo, key):
    _file = repo.get_by_id(key)
    data = json.loads(_file.getvalue().decode("utf-8"))["data"]
    ids_and_articles = []
    for article in data:
        ids_and_articles.append({"article": article})
    return ids_and_articles
