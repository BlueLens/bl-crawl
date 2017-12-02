from __future__ import print_function

import uuid
import os
from multiprocessing import Process

import redis
import stylelens_product
from stylelens_product.rest import ApiException
from stylelens_product.models.version import Version
from stylelens_product.models.host import Host
from bluelens_spawning_pool import spawning_pool
from bluelens_log import Logging

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


CWD_PATH = os.getcwd()

REDIS_VERSION_CRAWL_QUEUE = 'bl:version:crawl:queue'
REDIS_HOST_CRAWL_QUEUE = 'bl:host:crawl:queue'

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
SUBSCRIBE_TOPIC = os.environ['SUBSCRIBE_TOPIC']
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-crawler')


def spawn_crawler(version):

  id = str(uuid.uuid4())
  pool = spawning_pool.SpawningPool()

  project_name = 'bl-crawlper-' + id
  log.debug('spawn_crawler: ' + project_name)

  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  pool.setApiVersion('v1')
  pool.setKind('Pod')
  pool.setMetadataName(project_name)
  pool.setMetadataNamespace('index')
  pool.addMetadataLabel('name', project_name)
  pool.addMetadataLabel('group', 'bl-crawler')
  pool.addMetadataLabel('SPAWN_ID', id)
  container = pool.createContainer()
  pool.setContainerName(container, project_name)
  pool.addContainerEnv(container, 'REDIS_SERVER', REDIS_SERVER)
  pool.addContainerEnv(container, 'REDIS_PASSWORD', REDIS_PASSWORD)
  pool.addContainerEnv(container, 'SPAWN_ID', id)
  pool.addContainerEnv(container, 'PRODUCT_VERSION', version)
  pool.setContainerImage(container, 'bluelens/bl-crawler:latest')
  pool.addContainer(container)
  pool.setRestartPolicy('Never')
  pool.spawn()

def create_new_version(version_name):
  log.info("create_new_version:" + version_name)
  version_api = stylelens_product.VersionApi()
  version = Version()
  version.version_name = version_name

  try:
    res = version_api.add_version(version)
    if res.data != None and res.data.version_id != None:
      return True
  except ApiException as e:
    log.error("Exception when calling VersionApi->add_version: %s\n" % e)
    return False

  return False

def push_host_to_queue(host_code):
  rconn.lpush(REDIS_HOST_CRAWL_QUEUE, host_code)

def start_crawl(version):
  log.info('start_crawl')
  host_api = stylelens_product.HostApi()
  host = Host()

  offset = 0
  limit = 100
  try:
    while True:
      res = host_api.get_hosts(offset=offset, limit=limit)
      for h in res.data:
        push_host_to_queue(h.host_code)
        spawn_crawler(version)

      if limit > len(res.data):
        break
      else:
        offset = offset + limit
  except ApiException as e:
    log.error("Exception when calling HostApi->get_hosts: %s\n" % e)

def dispatch(rconn):
  while True:
    key, value = rconn.blpop([REDIS_VERSION_CRAWL_QUEUE])
    version = value.decode('utf-8')
    ret = create_new_version(version)
    if ret == True:
      start_crawl(version)

if __name__ == '__main__':
  log.info('Start bl-crawl')
  try:
    Process(target=dispatch, args=(rconn,)).start()
  except Exception as e:
    log.error(str(e))
    exit()
