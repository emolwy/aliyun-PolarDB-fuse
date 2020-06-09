# coding=utf-8
import polardb, redis, json, ast, logging, time
import requests as req
from components import db
from components import config
from components import aliyun_polardb_api

logging.basicConfig(filename='/aliyun-polardb-fuse/logs/system.log', level=logging.DEBUG, format='%(asctime)s - [%(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# 通过企业微信机器人发送信息
def sendMsg(message):
    headers = {'Content-Type: application/json;charset=utf-8'}
    wxwork_robot_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key="+config.get('CONFIG.WXWORK_ROBOT_KEY')
    wxwork_text_json = {
    "msgtype": "text",
    "text": {
        "content": message,
        "mentioned_list": json.loads(config.get('CONFIG.WXWORK_USER_LIST')),
        }
    }
    reqContent = str(req.post(wxwork_robot_url, json.dumps(wxwork_text_json), headers).content)
    logging.debug('Function sendMsg():'+reqContent)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' Function sendMsg():'+reqContent)

# 查询是否存在慢SQL，如存在，返回这些慢SQL所属的用户。
def querySlowSQL(DBClusterId):

	SQL = 'SHOW PROCESSLIST;'

	logging.info('Function:querySlowSQL(), DBClusterId: ['+DBClusterId+']')
	print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' Function:querySlowSQL(), DBClusterId: ['+DBClusterId+']')

	try:
		dbconn = db.mysqlClient(
			polardb.getDBClusterEndpoints(DBClusterId),
			config.get('CONFIG.DB_USERNAME'),
			config.get('CONFIG.DB_PASSWORD'),
			'mysql')
		dbconn.dbconnect()
		dbconn.dbcursor()

		processlist_results = dbconn.dbquery(SQL)
	except Exception:
		logging.error('Function: querySlowSQL(), MySQLException.', exc_info = True)
		print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' Function: querySlowSQL(), MySQLException.')
	else:
		MysqlUserList = []

		# 遍历SHOW PROCESSLIST的结果
		for x in range(len(processlist_results)):
			for y in range(len(processlist_results[x])):
				# 查找哪些用户存在慢SQL，COMMAND为'Query'，执行时长超过预算阈值，且SQL语句为SELECT开头的。
				if processlist_results[x][y] == 'Query' and processlist_results[x][5] >= int(config.get('CONFIG.SLOWSQL_QUERY_TIME')) and str(processlist_results[x][7]).startswith('select'):
					if processlist_results[x][1] not in MysqlUserList:
						MysqlUserList.append(processlist_results[x][1])

		dbconn.dbclose()
		return MysqlUserList
		logging.info('Function:querySlowSQL(), MysqlUserList: ['+str(MysqlUserList)+']')
		print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' Function:querySlowSQL(), MysqlUserList: ['+str(MysqlUserList)+']')

# 查杀某个用户下的慢SQL进程，并返回慢SQL的数量	
def killSlowSQL(DBClusterId, MYSQL_USER):

	SQL = 'SHOW PROCESSLIST;'

	logging.info('Function:killSlowSQL(), DBClusterId: ['+DBClusterId+'], 当前数据库账号：['+MYSQL_USER+']')
	print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' Function:killSlowSQL(), DBClusterId: ['+DBClusterId+'], 当前数据库账号：['+MYSQL_USER+']')

	try:
		dbconn = db.mysqlClient(
			polardb.getDBClusterEndpoints(DBClusterId),
			MYSQL_USER,
			polardb.getUserPassword(MYSQL_USER),
			'mysql')
		dbconn.dbconnect()
		dbconn.dbcursor()

		processlist_results = dbconn.dbquery(SQL)
	except Exception:
		logging.error('Function: killSlowSQL(), MySQLException.', exc_info = True)
		print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' Function: killSlowSQL(), MySQLException.')
	else:
		SLOWSQL_CONNECTION_COUNT = 0
		SlowSqlIdList = []

		# 遍历SHOW PROCESSLIST的结果
		for x in range(len(processlist_results)):
			for y in range(len(processlist_results[x])):
				# 查找及查杀用户所属的慢SQL，COMMAND为'Query'，执行时长超过预算阈值，且SQL语句为SELECT开头的。
				if processlist_results[x][y] == 'Query' and processlist_results[x][1] == MYSQL_USER and processlist_results[x][5] >= int(config.get('CONFIG.SLOWSQL_QUERY_TIME')) and str(processlist_results[x][7]).startswith('select'):
					SLOWSQL_CONNECTION_COUNT += 1
					SlowSqlIdList.append(processlist_results[x][0])
					logging.info('Function:killSlowSQL(), 慢SQL进程ID['+str(processlist_results[x][0]+'], SQL：'+str(processlist_results[x][7]))
					print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' Function:killSlowSQL(), 慢SQL进程ID['+str(processlist_results[x][0])+'], SQL：'+str(processlist_results[x][7]))
		# 慢SQL数量超过预设值，则KILL。
		if  SLOWSQL_CONNECTION_COUNT >= int(config.get('CONFIG.SLOWSQL_CONNECTION_COUNT')):
			for i in range(len(SlowSqlIdList)):
				SQL = 'KILL ' + str(SlowSqlIdList[i]) + ';'
				dbconn.dbexecute(SQL)
				logging.info('Function:killSlowSQL(), 已查杀慢SQL进程: KILL '+str(SlowSqlIdList[i]))
				print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' Function:killSlowSQL(), 已查杀慢SQL进程: KILL '+str(SlowSqlIdList[i]))
		else:
			logging.info('Function:killSlowSQL(), 慢SQL数量未超过预设值：SLOWSQL_CONNECTION_COUNT')
			print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+'Function:killSlowSQL(), 慢SQL数量未超过预设值：SLOWSQL_CONNECTION_COUNT')
		dbconn.dbclose()
		return SLOWSQL_CONNECTION_COUNT


def onPolardbInspection():

	# 初始化阿里云PolarDB API客户端
	aliClient =  aliyun_polardb_api.polardbClient(
	config.get('CONFIG.accessKeyId'),
	config.get('CONFIG.accessSecret'),
	config.get('CONFIG.regionId'))

	DBClusterIdList = polardb.getDBClusterIdList()

	# 巡检PolarDB集群节点的资源使用情况，涉及CPU、IOPS和连接数三个指标。
	for x in range(len(DBClusterIdList)):

		NodesList = polardb.getDBNodesIdList(DBClusterIdList[x])
		NodeClass = polardb.getDBNodeClass(DBClusterIdList[x]).split('.')[3]

		for y in range(len(NodesList)):
			logging.info('Function:onPolardbInspection(), 集群： ['+polardb.getDBClusterDescription(DBClusterIdList[x])+'], 集群节点： ['+NodesList[y]+']')
			print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' Function:onPolardbInspection(), 集群： ['+polardb.getDBClusterDescription(DBClusterIdList[x])+'], 集群节点： ['+NodesList[y]+']')
			try:
				NodeInspenction = aliClient.getDBNodePerformance(NodesList[y])
			except Exception:
				logging.error('Function:onPolardbInspection(), AliyunApiException:', exc_info=True)
				print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' Function onPolardbInspection(): AliyunApiException')
			else:
				# 如PolarDB节点的CPU、IOPS和连接数，其中一个监控指标超过预设阈值，开始查找及查杀慢SQL
				if NodeInspenction['cpu_ratio'] >= int(config.get('CONFIG.CPU_USAGE')) \
				or round(NodeInspenction['mean_total_session'] / ast.literal_eval(config.get('CONFIG.'+NodeClass))['Max_Connection'] * 100, 2) >= int(config.get('CONFIG.CONNECTION_USAGE')) \
				or round(NodeInspenction['mean_iops'] / ast.literal_eval(config.get('CONFIG.'+NodeClass))['Max_IOPS'] * 100, 2) >= int(config.get('CONFIG.IOPS_USAGE')) :
					MysqlUserList = querySlowSQL(DBClusterIdList[x])
					if MysqlUserList:
						for z in MysqlUserList:
							SLOWSQL_COUNT = killSlowSQL(DBClusterIdList[x], z)
							message = '''[发怒]PolarDB慢SQL(>={time}秒)熔断告警
集群：{DBClusterId}, 
集群节点：{NodeId}, 
CPU：{cpu}%, 
IOPS：{iops}, 使用率：{iops_usage}%
连接数：{session}, 使用率：{session_usage}%
提示信息：发现慢SQL数量：{count}，且已自动操作查杀，请关注业务异常情况。
'''.format(time=config.get('CONFIG.SLOWSQL_QUERY_TIME'),
	DBClusterId=polardb.getDBClusterDescription(DBClusterIdList[x]),
	NodeId=NodesList[y],
	cpu=NodeInspenction['cpu_ratio'],
	iops=NodeInspenction['mean_iops'],
	iops_usage=round(NodeInspenction['mean_iops'] / ast.literal_eval(config.get('CONFIG.'+NodeClass))['Max_IOPS'] * 100, 2),
	session=NodeInspenction['mean_total_session'],
	session_usage=round(NodeInspenction['mean_total_session'] / ast.literal_eval(config.get('CONFIG.'+NodeClass))['Max_Connection'] * 100, 2),
	count=SLOWSQL_COUNT)
							# 企业微信机器人通知相关人员
							sendMsg(message)
					else:
						logging.info('Function:onPolardbInspection(), 集群： ['+polardb.getDBClusterDescription(DBClusterIdList[x])+'], 集群节点： ['+NodesList[y]+']。 暂无慢SQL，集群节点资源使用情况 [ CPU：'+str(NodeInspenction['cpu_ratio'])+', IOPS：'+str(NodeInspenction['mean_iops'])+', 连接数：'+str(NodeInspenction['mean_total_session']))
						print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' Function:onPolardbInspection(), 集群： ['+polardb.getDBClusterDescription(DBClusterIdList[x])+'], 集群节点： ['+NodesList[y]+']。 暂无慢SQL，集群节点资源使用情况 [ CPU：'+str(NodeInspenction['cpu_ratio'])+', IOPS：'+str(NodeInspenction['mean_iops'])+', 连接数：'+str(NodeInspenction['mean_total_session']))
				else:
					logging.info('Function:onPolardbInspection(), 集群： ['+polardb.getDBClusterDescription(DBClusterIdList[x])+'], 集群节点： ['+NodesList[y]+']。 资源正常，集群节点资源使用情况 [ CPU：'+str(NodeInspenction['cpu_ratio'])+', IOPS：'+str(NodeInspenction['mean_iops'])+', 连接数：'+str(NodeInspenction['mean_total_session']))
					print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' Function:onPolardbInspection(), 集群： ['+polardb.getDBClusterDescription(DBClusterIdList[x])+'], 集群节点： ['+NodesList[y]+']。 资源正常，集群节点资源使用情况 [ CPU：'+str(NodeInspenction['cpu_ratio'])+', IOPS：'+str(NodeInspenction['mean_iops'])+', 连接数：'+str(NodeInspenction['mean_total_session']))
			#休眠5秒，避免请求阿里云API的并发过多，导致请求报400错误。
			time.sleep(5)

# 执行
if __name__ == '__main__':
	onPolardbInspection()

