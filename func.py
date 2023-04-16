from datetime import datetime, timedelta


class Aggregate:

    # Группировка данных за месяц
    @staticmethod
    def get_dataset_month(collection, dt_from: str, dt_upto: str):

        cursor = collection.aggregate([
            {
                "$match": {
                    'dt': {
                        '$gte': datetime.fromisoformat(dt_from),
                        '$lte': datetime.fromisoformat(dt_upto)
                    }
                }
            },
            {
                '$addFields': {
                    'group_type': 'month',
                    'label': {'$dateFromParts': {'year': {'$year': "$dt"}, 'month': {'$month': '$dt'}}}
                }
            },
            {
                '$group': {
                    '_id': {'$month': '$dt'},
                    'values': {'$sum': '$value'},
                    'label': {'$min': '$label'}
                }
            },
            {
                '$sort': {
                    '_id': 1
                }
            },
            {
                '$group': {
                    '_id': '$group_type',
                    'dataset': {'$push': '$values'},
                    'labels': {'$push': {'$dateToString': {'date': '$label', 'format': '%Y-%m-%dT%H:%M:%S'}}}
                }
            },
            {
                '$project': {
                    '_id': 0,

                }
            },
        ])

        for doc in cursor:
            return doc

    # Группировка данных за день
    @staticmethod
    def get_dataset_day(collection, dt_from: str, dt_upto: str):

        dates = []
        delta = timedelta(days=1)
        date_start = datetime.fromisoformat(dt_from)
        date_end = datetime.fromisoformat(dt_upto) + timedelta(days=1)
        while date_start <= date_end:
            dates.append(date_start)
            date_start += delta

        cursor = collection.aggregate([
            {
                "$match": {
                    "dt": {
                        "$gte": datetime.fromisoformat(dt_from),
                        "$lte": datetime.fromisoformat(dt_upto)
                    }
                }

            },
            {
                "$addFields": {
                    "group_type": "day",
                    "label": {"$dateTrunc": {"date": "$dt", "unit": "day"}}
                }
            },
            {
                "$bucket": {
                    "groupBy": "$dt",
                    "boundaries": dates,
                    "default": "Other",
                    "output": {
                        "values": {"$sum": "$value"},
                        "label": {"$min": "$label"}
                    }
                }
            },
            {
                "$group": {
                    "_id": "$group_type",
                    "dataset": {"$push": "$values"},
                    "labels": {"$push": {"$dateToString": {"date": "$label", "format": "%Y-%m-%dT%H:%M:%S"}}}
                }
            },
            {
                "$project": {
                    "_id": 0,

                }
            },
        ])

        for doc in cursor:
            return doc

