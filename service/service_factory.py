from __future__ import absolute_import
from .amazon import Amazon
from .etc_mall import EtcMall


class ServiceFactory(object):
  def __init__(self, redis, log):
    self._services = []
    self._log = log
    self._redis = redis

  def run_service(self, host_code, host_group, version_id):
    if host_code == 'HC8000':
      self._services.append(Amazon(self._redis, self._log))
    else:
      self._services.append(EtcMall(self._redis, self._log))

    self._run(host_code, host_group, version_id)

  def _run(self, host_code, host_group, version_id):
    for s in self._services:
      s.do(host_code, host_group, version_id)

