from datetime import datetime


class Aggregate:

    # Группировка данных за месяц
    @staticmethod
    def get_dataset_month(collection, dt_from: str, dt_upto: str):

        cursor = collection.aggregate([
            {
                '$match': {
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
            {   '$group': {
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

        cursor = collection.aggregate([
            {
                '$match': {
                    'dt': {
                        '$gte': datetime.fromisoformat(dt_from),
                        '$lte': datetime.fromisoformat(dt_upto)
                    }
                }

            },
            {
                '$addFields': {
                    'group_type': 'day',
                    'label': {'$dateFromParts': {'year': {'$year': "$dt"}, 'month': {'$month': '$dt'}}}
                }
            },
            {
                '$group': {
                    '_id': {
                        'group_type': {'$dateToString': {'format': "%Y-%m-%d", 'date': "$dt"}}
                    },
                    'dataset': {'$sum': '$value'},
                    'label': {'$min': '$dt'}
                }
            },
            {
                '$sort': {
                    '_id': 1
                }
            },
            {   '$group': {
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

