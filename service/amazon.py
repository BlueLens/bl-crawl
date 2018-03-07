from __future__ import absolute_import

import os
import pickle
import time
from bluelens_k8s.service import Service
from stylelens_crawl_amazon.stylelens_crawl import StylensCrawler

REDIS_CRAWL_AMZ_QUEUE = "bl:crawl:amz:queue"

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
RELEASE_MODE = os.environ['RELEASE_MODE']
DB_PRODUCT_HOST = os.environ['DB_PRODUCT_HOST']
DB_PRODUCT_PORT = os.environ['DB_PRODUCT_PORT']
DB_PRODUCT_USER = os.environ['DB_PRODUCT_USER']
DB_PRODUCT_PASSWORD = os.environ['DB_PRODUCT_PASSWORD']
DB_PRODUCT_NAME = os.environ['DB_PRODUCT_NAME']
AWS_ASSOCIATE_TAG = os.environ['AWS_ASSOCIATE_TAG']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']


SPAWN_MAX = 4000

class Amazon(Service):
  def __init__(self, redis, log):
    super(Amazon, self).__init__(redis, log)

    print('Amazon: init')
    self._sc = StylensCrawler(generate_item_searches=True)
    self._redis = redis
    self._log = log

  def do(self, host_code, host_group, version_id):
    item_searches = self._sc.get_item_searches()

    self._spawn_ticker()

    count = 0
    i = 0

    if 0 < self._redis.llen(REDIS_CRAWL_AMZ_QUEUE):
      spawn_flag = False
    else:
      spawn_flag = True

    for search in item_searches:
      search_data = search.search_data
      data_dic = search_data.to_dict()
      if spawn_flag == True:
        self._redis.lpush(REDIS_CRAWL_AMZ_QUEUE, pickle.dumps(data_dic))
      count = count + 1
      if count % SPAWN_MAX == 0:
        self._spawn_crawler(str(i), host_code, host_group, version_id)
        i = i + 1

  def _spawn_ticker(self):
    envs = {}
    envs['TICKER_KEY'] = 'bl:ticker:crawl:amazon'
    envs['TICKER_VALUE'] = '1' # 1 sec
    envs['REDIS_SERVER'] = REDIS_SERVER
    envs['REDIS_PASSWORD'] = REDIS_PASSWORD
    self.spawn_crawler(container_name='bl-ticker',
                       container_image='bluelens/bl-ticker:' + RELEASE_MODE,
                       id='crawl',
                       server_url=REDIS_SERVER,
                       server_password=REDIS_PASSWORD,
                       metadata_namespace=RELEASE_MODE,
                       envs=envs)

  def _spawn_crawler(self, id, host_code, host_group, version_id):
    envs = {}
    envs['HOST_GROUP'] = host_group
    envs['HOST_CODE'] = host_code
    envs['VERSION_ID'] = version_id
    envs['SPAWN_ID'] = id
    envs['REDIS_SERVER'] = REDIS_SERVER
    envs['REDIS_PASSWORD'] = REDIS_PASSWORD
    envs['RELEASE_MODE'] = RELEASE_MODE
    envs['DB_PRODUCT_HOST'] = DB_PRODUCT_HOST
    envs['DB_PRODUCT_PORT'] = DB_PRODUCT_PORT
    envs['DB_PRODUCT_USER'] = DB_PRODUCT_USER
    envs['DB_PRODUCT_PASSWORD'] = DB_PRODUCT_PASSWORD
    envs['DB_PRODUCT_NAME'] = DB_PRODUCT_NAME
    envs['AWS_ASSOCIATE_TAG'] = AWS_ASSOCIATE_TAG
    envs['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY_ID
    envs['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_ACCESS_KEY
    self.spawn_crawler(container_name='bl-crawler',
                       container_image='bluelens/bl-crawler:' + RELEASE_MODE,
                       id=id,
                       server_url=REDIS_SERVER,
                       server_password=REDIS_PASSWORD,
                       metadata_namespace=RELEASE_MODE,
                       envs=envs)
