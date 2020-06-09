# coding = utf-8
import redis
import json, ast
from components import aliyun_polardb_api
from components import config
print(config.get('CONFIG.REDIS_PASS'))
conn = redis.StrictRedis(host=config.get('CONFIG.REDIS_HOST'), 
	port=config.get('CONFIG.REDIS_PORT'), 
	password=config.get('CONFIG.REDIS_PASS'),
	db=0,decode_responses=True)
aliClient =  aliyun_polardb_api.polardbClient(
	config.get('CONFIG.accessKeyId'),
	config.get('CONFIG.accessSecret'),
	config.get('CONFIG.regionId'))
DBClusterInfoAllList = aliClient.getDBClusterInfoAllList()
conn.set("DBClusterInfoAll", json.dumps(DBClusterInfoAllList))

UsersDict = {'USERNAME1': 'PASSWORD1', 'USERNAME2': 'PASSWORD2', ......}
conn.set("UsersDict", json.dumps(UsersDict))


