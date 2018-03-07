from __future__ import absolute_import
import os
from bluelens_k8s.service import Service

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

class EtcMall(Service):
  def __init__(self, redis, log):
    super(EtcMall, self).__init__(redis, log)
    print('EtcMall: init')
    self._redis = redis
    self._log = log

  def do(self, host_code, host_group, version_id):
    id = host_code.lower()
    self._spawn(id, host_code, host_group, version_id)

  def _spawn(self, id, host_code, host_group, version_id):
    envs = {}
    envs['HOST_GROUP'] = host_group
    envs['HOST_CODE'] = host_code
    envs['VERSION_ID'] = version_id
    envs['SPAWN_ID'] = str(id)
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
                       id=str(id),
                       server_url=REDIS_SERVER,
                       server_password=REDIS_PASSWORD,
                       metadata_namespace=RELEASE_MODE,
                       envs=envs)
