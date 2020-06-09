# coding=utf-8

from components import config
import redis, json
		
# Redis Connection Pool
POOL = redis.ConnectionPool(
	host=config.get('CONFIG.REDIS_HOST'), 
	port=config.get('CONFIG.REDIS_PORT'), 
	password=config.get('CONFIG.REDIS_PASS'),
	max_connections=10,db=0)
CONN = redis.StrictRedis(connection_pool=POOL, decode_responses = True)
DBClusterInfoAllJson = json.loads(CONN.get("DBClusterInfoAll"))

def getDBClusterIdList():
	DBClusterIdList = []	
	for i in range(len(DBClusterInfoAllJson)):
		DBClusterIdList.append(DBClusterInfoAllJson[i]['DBClusterId'])
	return DBClusterIdList

def getDBNodesIdList(DBClusterId):
	for i in range(len(DBClusterInfoAllJson)):
		if DBClusterInfoAllJson[i]['DBClusterId'] == DBClusterId:
			return DBClusterInfoAllJson[i]['DBNodes']
			break

def getDBClusterEndpoints(DBClusterId):
	for i in range(len(DBClusterInfoAllJson)):
		if DBClusterInfoAllJson[i]['DBClusterId'] == DBClusterId:
			return DBClusterInfoAllJson[i]['ConnectionString']
			break
def getDBClusterDescription(DBClusterId):
	for i in range(len(DBClusterInfoAllJson)):
		if DBClusterInfoAllJson[i]['DBClusterId'] == DBClusterId:
			return DBClusterInfoAllJson[i]['DBClusterDescription']
			break
			
def getDBNodeClass(DBClusterId):
	for i in range(len(DBClusterInfoAllJson)):
		if DBClusterInfoAllJson[i]['DBClusterId'] == DBClusterId:
			return DBClusterInfoAllJson[i]['DBNodeClass']
			break

def getUserPassword(MYSQL_USER):

	if MYSQL_USER in json.loads(CONN.get('UsersDict')):
		return json.loads(CONN.get('UsersDict'))[MYSQL_USER]
	else:
		raise Exception('KeyError: '+ MYSQL_USER)

