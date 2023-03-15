from elasticsearch import helpers
from pymongo import MongoClient
from elasticsearch import Elasticsearch
import os
import json
from datetime import datetime, date


def migrate():

  config = load_config()

  # Mongo Config
  client = MongoClient(config['mongodb']['uri'])
  db =  client[config['mongodb']['database']]
  collection = db[config['mongodb']['collection']]

  # Elasticsearch Config
  es_host = config['elasticsearch']['host']
  username = config['elasticsearch']['username']
  password = config['elasticsearch']['password']
  es = Elasticsearch([es_host], http_auth=(username, password))
  es_index = config['elasticsearch']['index']

  res = collection.find()

  # number of docs to migrate
  num_docs = 2000
  actions = []

  for doc in res:
      mongo_id = str(doc['_id'])
     # print(mongo_id)
      doc.pop('_id', None)
      doc['id'] = mongo_id
      actions.append({
          "_index": es_index,
          "_type" : es_index,
          "_id": str(mongo_id),
          "_source": json.dumps(doc, default = defaultconverter)
      })

  helpers.bulk(es, actions)
  print("import succesful")

def load_config():
    with open("config.json") as config_file:
        config = json.load(config_file)
    return config


def defaultconverter(o):
  if isinstance(o, (datetime, date)):
     return o.isoformat()


if __name__ == "__main__":
   migrate()