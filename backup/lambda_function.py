import pymongo
import os
import boto3
import datetime
from bson.json_util import dumps

# MongoDB
MONGO_CONNECTION_URL = os.environ.get('MONGO_CONNECTION_URL')
DATABASE_NAME = os.environ.get('DATABASE_NAME')
# S3
AWS_S3_BUCKET_NAME = os.environ.get('AWS_S3_BUCKET_NAME')


def backup_mongodb():
    client = pymongo.MongoClient(MONGO_CONNECTION_URL)
    db = client[DATABASE_NAME]

    collections = db.list_collection_names()

    backup_data = {}

    for collection_name in collections:
        collection = db[collection_name]
        backup_data[collection_name] = list(collection.find())

    filename = f"mongodb_backup_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"

    with open(filename, 'w') as file:
        file.write(dumps(backup_data))

    s3_client = boto3.client('s3')
    s3_client.upload_file(filename, AWS_S3_BUCKET_NAME, filename)

    print(f"Backup successful: {filename}")
