import redis
import json

data = {}
data['code'] = 'sc0000001'
data['name'] = '8seconds'


REDIS_SERVER = '35.187.244.252'
rconn = redis.StrictRedis(REDIS_SERVER, port=6379)

data_json_str = json.dumps(data)
print(data_json_str)
rconn.publish('crawl/done', data_json_str)

