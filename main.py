from __future__ import print_function

import uuid
import os
import time
from multiprocessing import Process

import redis
from stylelens_product.versions import Versions
from stylelens_product.hosts import Hosts
from stylelens_product.crawls import Crawls

from bluelens_log import Logging
from service.service_factory import ServiceFactory

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

rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}

log = Logging(options, tag='bl-crawl')

class Crawl(object):
  def __init__(self):
    self.crawl_api = Crawls()
    self.service_factory = ServiceFactory(rconn, log)


  def create_new_version(self, host_group):
    log.info("create_new_version:" + host_group)
    version_api = Versions()
    version = {}
    version['host_group'] = host_group

    try:
      res = version_api.add_version(version)
      if res is not None:
        version_id = res
        self.set_latest_crawl_version(version_id)
        return version_id
    except Exception as e:
      log.error("Exception when calling VersionApi->add_version: %s\n" % e)

    log.error("Could not create new version")
    return None

  def restart_indexer(self, version_id):
    rconn.lpush(REDIS_INDEX_RESTART_QUEUE, version_id)


  def set_latest_crawl_version(self, version_id):
    log.debug('set_latest_crawl_version : ' + version_id)
    rconn.hset(REDIS_CRAWL_VERSION, REDIS_CRAWL_VERSION_LATEST, version_id)

  def start_crawl(self, version_id, host_group):
    log.info('start_crawl')

    global host_api
    offset = 0
    limit = 30
    try:
      while True:

        crawls = self.crawl_api.get_crawls(version_id=version_id,
                                      host_group=host_group,
                                      status='todo',
                                      offset=offset,
                                      limit=limit)
        if len(crawls) == 0:
          break
        for crawl in crawls:
          host_code = crawl.get('host_code')
          self.dispatch_service(host_code, host_group, version_id)

        time.sleep(CRAWL_TERM)

    except Exception as e:
      log.error("Exception on start_crawl %s\n" % e)

  def dispatch_service(self, host_code, host_group, version_id):
    self.service_factory.run_service(host_code, host_group, version_id)

  def create_crawl_jobs(self, version_id, host_group):
    log.info('create_crawl_jobs')
    host_api = Hosts()

    offset = 0
    limit = 50

    while True:
      try:
        hosts = host_api.get_hosts(host_group=host_group,
                                   offset=offset, limit=limit)

        for host in hosts:
          crawl = {}
          crawl['host_code'] = host.get('host_code')
          crawl['host_group'] = host_group
          crawl['version_id'] = version_id
          self.crawl_api.add_crawl(crawl)

        if limit > len(hosts):
          break
        else:
          offset = offset + limit

      except Exception as e:
        log.error(str(e))

  def dispatch(self):
    while True:
      key, value = rconn.blpop([REDIS_JOB_CRAWL_QUEUE])
      host_group = value.decode('utf-8')
      version_id= self.create_new_version(host_group)
      if version_id is not None:
        self.create_crawl_jobs(version_id, host_group)
        self.restart_indexer(version_id)
        self.start_crawl(version_id, host_group)

if __name__ == '__main__':
  log.info('Start bl-crawl:2')
  try:
    crawl = Crawl()
    crawl.dispatch()
  except Exception as e:
    log.error(str(e))
