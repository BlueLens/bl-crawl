from __future__ import absolute_import

import os
from bluelens_spawning_pool import spawning_pool

class Service(object):
  def __init__(self, redis, log):
    print('init')
    self._redis = redis
    self._log = log

  def do(self, host_code, host_group, version_id):
    print('Service')

  def spawn_crawler(self, id, host_code, host_group, version_id):
    pool = spawning_pool.SpawningPool()

    project_name = 'bl-crawler-' + id
    self._log.debug('spawn_crawler: ' + project_name)

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
    pool.addContainerEnv(container, 'HOST_GROUP', host_group)
    pool.addContainerEnv(container, 'VERSION_ID', version_id)
    pool.addContainerEnv(container, 'RELEASE_MODE', RELEASE_MODE)
    pool.addContainerEnv(container, 'DB_PRODUCT_HOST', DB_PRODUCT_HOST)
    pool.addContainerEnv(container, 'DB_PRODUCT_PORT', DB_PRODUCT_PORT)
    pool.addContainerEnv(container, 'DB_PRODUCT_USER', DB_PRODUCT_USER)
    pool.addContainerEnv(container, 'DB_PRODUCT_PASSWORD', DB_PRODUCT_PASSWORD)
    pool.addContainerEnv(container, 'DB_PRODUCT_NAME', DB_PRODUCT_NAME)
    pool.addContainerEnv(container, 'AWS_ASSOCIATE_TAG', AWS_ASSOCIATE_TAG)
    pool.addContainerEnv(container, 'AWS_ACCESS_KEY_ID', AWS_ACCESS_KEY_ID)
    pool.addContainerEnv(container, 'AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY)
    pool.setContainerImage(container, 'bluelens/bl-crawler:' + RELEASE_MODE)
    pool.setContainerImagePullPolicy(container, 'Always')
    pool.addContainer(container)
    pool.setRestartPolicy('Never')
    pool.spawn()


