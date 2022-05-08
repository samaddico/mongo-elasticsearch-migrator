from typing import List
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
  es = Elasticsearch([es_host])
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
          "_source": json.dumps(doc, default = default_converter)
      })

  #helpers.parallel_bulk(client=es, actions=actions, thread_count=5,chunk_size=2500, raise_on_exception=True)
  helpers.bulk(es, actions)
  print("import succesful")

def load_config():
    with open("config.json") as config_file:
        config = json.load(config_file)
    return config


def default_converter(o):
  if isinstance(o, (datetime, date)):
    return o.isoformat()


def exclude_fields(fields: List):
    return

def generate_id():
    return

def __get_batch_size():
    return 2000

def parse():
    return 2000

if __name__ == "__main__":
   migrate()
