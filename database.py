import pandas as pd
import pymongo
from bson.json_util import dumps

import config

client = pymongo.MongoClient(f"mongodb://{config.DB_HOST}:{config.DB_PORT}/")
db = client[config.DB_NAME]
collection = db[config.DB_COLLECTION]


async def calculate(dt_from, dt_upto, group_type):
	groups = {
		'month': ['%Y-%m-01T00:00:00', 'M'],
		'day': ['%Y-%m-%dT00:00:00', 'D'],
		'hour': ['%Y-%m-%dT%H:00:00', 'H']
	}

	date_range = pd.date_range(start=dt_from, end=dt_upto, freq=groups[group_type][1])
	# Запрос на агрегацию данных
	pipeline = [
		{
			"$match": {
				"dt": {
					"$gte": dt_from,
					"$lte": dt_upto
				}
			}
		},
		{
			"$group": {
				"_id": {
					"$dateToString": {
						"format": groups[group_type][0],
						"date": "$dt"
					}
				},
				"totalValue": {
					"$sum": "$value"
				}
			}
		},
		{
			"$sort": {
				"_id": 1
			}
		}
	]

	result = list(collection.aggregate(pipeline))
	data_dict = {entry["_id"]: entry["totalValue"] for entry in result}

	result_dict = {}

	# add missed items
	for date in date_range:
		date_str = date.strftime(groups[group_type][0])
		if date_str not in data_dict.keys():
			result_dict[date_str] = 0
		else:
			result_dict[date_str] = data_dict[date_str]

	# converting results
	dataset = list(result_dict.values())
	labels = list(result_dict.keys())

	response = {
		"dataset": dataset,
		"labels": labels
	}

	return dumps(response)
