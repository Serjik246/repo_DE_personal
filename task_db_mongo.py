from pymongo import MongoClient
#from pprint import pprint
from datetime import datetime, timedelta
import json
#import os

client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]
archive_collection = db["archive_user_events"]

today = datetime.today()
today_str = today.strftime("%Y-%m-%d")
today_for_task = datetime(2024,2,4,12,00,00)
thirty_days_ago = today_for_task - timedelta(days=30)
fourteen_days_ago = today_for_task - timedelta(days=14)
query_conditions = {
    "user_info.registration_date": {'$lt': thirty_days_ago},
    "event_time": {'$lt': fourteen_days_ago}
}
user_ids = []
users_archive = []
archived_users_count = 0

for col in collection.find(query_conditions):
    user_ids.append(col['user_id'])
    users_archive.append(col)

if len(users_archive) > 0:
    archive_collection.insert_many(users_archive)
    archived_users_count = len(users_archive)

    collection.delete_many(query_conditions)
    print(f'Кол-во перенесенных пользователей в архив: {archived_users_count}\n'
          f'{user_ids}')

    data_json = {
        "date": today_str,
        "archived_users_count": archived_users_count,
        "archived_user_ids": user_ids
    }

    with open(f'{today_str}.json', 'w', encoding='utf-8') as f:
        json.dump(data_json, f, ensure_ascii=False, indent=2)
    print(f'✅ Данные выгружены {today_str}.json')
else:
    print('❌ По заданным параметрам пользователи отсутствуют')