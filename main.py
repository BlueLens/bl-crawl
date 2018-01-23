from __future__ import print_function

import uuid
import os
import time
from multiprocessing import Process

import redis
from stylelens_product.versions import Versions
from stylelens_product.hosts import Hosts
from stylelens_product.crawls import Crawls

from bluelens_spawning_pool import spawning_pool
from bluelens_log import Logging

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


CWD_PATH = os.getcwd()

CRAWL_TERM = 60*30

HOST_STATUS_TODO = 'todo'
HOST_STATUS_DOING = 'doing'
HOST_STATUS_DONE = 'done'

REDIS_JOB_CRAWL_QUEUE = 'bl:job:crawl:queue'
REDIS_HOST_CRAWL_QUEUE = 'bl:host:crawl:queue'
REDIS_CRAWL_VERSION = 'bl:crawl:version'
REDIS_CRAWL_VERSION_LATEST = 'latest'
REDIS_INDEX_RESTART_QUEUE = "bl:index:restart:queue"

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
RELEASE_MODE = os.environ['RELEASE_MODE']
DB_PRODUCT_HOST = os.environ['DB_PRODUCT_HOST']
DB_PRODUCT_PORT = os.environ['DB_PRODUCT_PORT']
DB_PRODUCT_USER = os.environ['DB_PRODUCT_USER']
DB_PRODUCT_PASSWORD = os.environ['DB_PRODUCT_PASSWORD']
DB_PRODUCT_NAME = os.environ['DB_PRODUCT_NAME']
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-crawl')

crawl_api = Crawls()

def spawn_crawler(host_code, version_id):
  pool = spawning_pool.SpawningPool()
  id = host_code.lower()

  project_name = 'bl-crawler-' + id
  log.debug('spawn_crawler: ' + project_name)

  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  pool.setApiVersion('v1')
  pool.setKind('Pod')
  pool.setMetadataName(project_name)
  pool.setMetadataNamespace(RELEASE_MODE)
  pool.addMetadataLabel('name', project_name)
  pool.addMetadataLabel('group', 'bl-crawler')
  pool.addMetadataLabel('SPAWN_ID', id)
  container = pool.createContainer()
  pool.setContainerName(container, project_name)
  pool.addContainerEnv(container, 'REDIS_SERVER', REDIS_SERVER)
  pool.addContainerEnv(container, 'REDIS_PASSWORD', REDIS_PASSWORD)
  pool.addContainerEnv(container, 'SPAWN_ID', id)
  pool.addContainerEnv(container, 'HOST_CODE', host_code)
  pool.addContainerEnv(container, 'VERSION_ID', version_id)
  pool.addContainerEnv(container, 'RELEASE_MODE', RELEASE_MODE)
  pool.addContainerEnv(container, 'DB_PRODUCT_HOST', DB_PRODUCT_HOST)
  pool.addContainerEnv(container, 'DB_PRODUCT_PORT', DB_PRODUCT_PORT)
  pool.addContainerEnv(container, 'DB_PRODUCT_USER', DB_PRODUCT_USER)
  pool.addContainerEnv(container, 'DB_PRODUCT_PASSWORD', DB_PRODUCT_PASSWORD)
  pool.addContainerEnv(container, 'DB_PRODUCT_NAME', DB_PRODUCT_NAME)
  pool.setContainerImage(container, 'bluelens/bl-crawler:' + RELEASE_MODE)
  pool.setContainerImagePullPolicy(container, 'Always')
  pool.addContainer(container)
  pool.setRestartPolicy('Never')
  pool.spawn()

def create_new_version(version_name):
  log.info("create_new_version:" + version_name)
  version_api = Versions()
  version = {}
  version['name'] = version_name

  try:
    res = version_api.add_version(version)
    if res is not None:
      version_id = res
      set_latest_crawl_version(version_id)
      return version_id
  except Exception as e:
    log.error("Exception when calling VersionApi->add_version: %s\n" % e)

  log.error("Could not create new version")
  return None

def restart_indexer(version_id):
  rconn.lpush(REDIS_INDEX_RESTART_QUEUE, version_id)


def set_latest_crawl_version(version_id):
  log.debug('set_latest_crawl_version : ' + version_id)
  rconn.hset(REDIS_CRAWL_VERSION, REDIS_CRAWL_VERSION_LATEST, version_id)

def start_crawl(version_id):
  log.info('start_crawl')

  global host_api
  offset = 0
  limit = 30
  try:
    while True:

      crawls = crawl_api.get_crawls(version_id=version_id,
                                    status='todo',
                                    offset=offset,
                                    limit=limit)
      if len(crawls) == 0:
        break

      for crawl in crawls:
        spawn_crawler(crawl['host_code'], version_id)

      time.sleep(CRAWL_TERM)

  except Exception as e:
    log.error("Exception when calling HostApi->get_hosts: %s\n" % e)

def create_crawl_jobs(version_id):
  log.info('create_crawl_jobs')
  host_api = Hosts()

  offset = 0
  limit = 50

  while True:
    try:
      hosts = host_api.get_hosts(version_id=None, offset=offset, limit=limit)

      for host in hosts:
        crawl = {}
        crawl['host_code'] = host['host_code']
        crawl['version_id'] = version_id
        crawl_api.add_crawl(crawl)

      if limit > len(hosts):
        break
      else:
        offset = offset + limit

    except Exception as e:
      log.error(str(e))

def dispatch():
  while True:
    key, value = rconn.blpop([REDIS_JOB_CRAWL_QUEUE])
    version_name = value.decode('utf-8')
    version_id= create_new_version(version_name)
    if version_id is not None:
      create_crawl_jobs(version_id)
      restart_indexer(version_id)
      start_crawl(version_id)

if __name__ == '__main__':
  log.info('Start bl-crawl:3')
  try:
    dispatch()
  except Exception as e:
    log.error(str(e))
    # exit()
