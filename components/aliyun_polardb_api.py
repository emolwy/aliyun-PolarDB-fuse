# coding=utf-8

from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.acs_exception.exceptions import ClientException
from aliyunsdkcore.acs_exception.exceptions import ServerException
from aliyunsdkpolardb.request.v20170801.DescribeDBClustersRequest import DescribeDBClustersRequest
from aliyunsdkpolardb.request.v20170801.DescribeDBClusterAttributeRequest import DescribeDBClusterAttributeRequest
from aliyunsdkpolardb.request.v20170801.DescribeDBClusterEndpointsRequest import DescribeDBClusterEndpointsRequest
from aliyunsdkpolardb.request.v20170801.DescribeDBClusterPerformanceRequest import DescribeDBClusterPerformanceRequest
from aliyunsdkpolardb.request.v20170801.DescribeDBNodePerformanceRequest import DescribeDBNodePerformanceRequest
import json
import datetime
from datetime import datetime as dt

class polardbClient(object):
	def __init__(self, accessKeyId, accessSecret, regionId):
		self.accessKeyId = accessKeyId
		self.accessSecret = accessSecret
		self.regionId = regionId

		self.client = AcsClient(self.accessKeyId, self.accessSecret, self.regionId)

	def getDBClusterEndpoints(self, DBClusterId):
		ConnectionString = None

		request = DescribeDBClusterEndpointsRequest()
		request.set_accept_format('json')
		request.set_DBClusterId(DBClusterId)
		response = self.client.do_action_with_exception(request)

		for i in range(len((json.loads(response))['Items'])):
			if (json.loads(response))['Items'][i]['EndpointType'] == 'Cluster':
				ConnectionString = (json.loads(response))['Items'][i]['AddressItems'][0]['ConnectionString']
				break
		return ConnectionString

	def getDBClusterIdList(self):

		request = DescribeDBClustersRequest()
		request.set_accept_format('json')
		response = self.client.do_action_with_exception(request)
		DBClusterInfoJson = (json.loads(response))['Items']['DBCluster']

		DBClusterIdList = []
		for i in range(len(DBClusterInfoJson)):
			DBClusterIdList.append(DBClusterInfoJson[i]['DBClusterId'])
		return DBClusterIdList

	def getDBNodesIdList(self, DBClusterId):
		request = DescribeDBClustersRequest()
		request.set_accept_format('json')
		response = self.client.do_action_with_exception(request)
		DBClusterInfoJson = (json.loads(response))['Items']['DBCluster']

		for i in range(len(DBClusterInfoJson)):
			if DBClusterInfoJson[i]['DBClusterId'] == DBClusterId:
				DBNodesIdList = []
				for j in range(len(DBClusterInfoJson[i]['DBNodes']['DBNode'])):
					DBNodesIdList.append(DBClusterInfoJson[i]['DBNodes']['DBNode'][j]['DBNodeId'])
				break
		return DBNodesIdList

	def getDBClusterInfoList(self, DBClusterId):

		request = DescribeDBClustersRequest()
		request.set_accept_format('json')
		response = self.client.do_action_with_exception(request)
		DBClusterInfoJson = (json.loads(response))['Items']['DBCluster']

		DBClusterInfoList = []
		DBClusterInfoDict = {}
		for i in range(len(DBClusterInfoJson)):
			if DBClusterInfoJson[i]['DBClusterId'] == DBClusterId:
				DBClusterInfoDict['DBClusterId'] = DBClusterInfoJson[i]['DBClusterId']
				DBClusterInfoDict['DBClusterDescription'] = DBClusterInfoJson[i]['DBClusterDescription']
				DBClusterInfoDict['DBNodeClass'] = DBClusterInfoJson[i]['DBNodeClass']
				DBClusterInfoDict['ConnectionString'] = self.getDBClusterEndpoints(DBClusterInfoJson[i]['DBClusterId'])

				DBNodesList = []
				for j in range(len(DBClusterInfoJson[i]['DBNodes']['DBNode'])):
					DBNodesList.append(DBClusterInfoJson[i]['DBNodes']['DBNode'][j]['DBNodeId'])
					DBClusterInfoDict['DBNodes'] = DBNodesList
				break
			DBClusterInfoList.append(DBClusterInfoDict)
		return DBClusterInfoList

	def getDBClusterInfoAllList(self):
		DBClusterInfoAllList = []

		request = DescribeDBClustersRequest()
		request.set_accept_format('json')
		response = self.client.do_action_with_exception(request)
		DBClusterInfoJson = (json.loads(response))['Items']['DBCluster']
		#print((json.loads(response))['Items']['DBCluster'])
		for DBClusterInfoJson in (json.loads(response))['Items']['DBCluster']:
			DBClusterDict = {}
			DBNodesList = []
			DBClusterDict['DBClusterId'] = DBClusterInfoJson['DBClusterId']
			DBClusterDict['DBClusterDescription'] = DBClusterInfoJson['DBClusterDescription']
			DBClusterDict['DBNodeClass'] = DBClusterInfoJson['DBNodeClass']
			DBClusterDict['ConnectionString'] = self.getDBClusterEndpoints(DBClusterInfoJson['DBClusterId'])
			for i in range(len(DBClusterInfoJson['DBNodes']['DBNode'])):
				DBNodesList.append(DBClusterInfoJson['DBNodes']['DBNode'][i]['DBNodeId'])
				DBClusterDict['DBNodes'] = DBNodesList
			DBClusterInfoAllList.append(DBClusterDict)

		return DBClusterInfoAllList

	def getDBNodePerformance(self, DBNodeId):

		# UTC时间转换，格式：%Y-%m-%dT%H:%MZ
		EndTime = dt.strftime((dt.today() - datetime.timedelta(hours=8)), '%Y-%m-%dT%H:%MZ')
		StartTime = dt.strftime((dt.today() - datetime.timedelta(hours=8) - datetime.timedelta(minutes=2)), '%Y-%m-%dT%H:%MZ')

		request = DescribeDBNodePerformanceRequest()
		request.set_accept_format('json')

		request.set_DBNodeId(DBNodeId)
		request.set_Key("PolarDBCPU,PolarDBConnections,PolarDBIOSTAT")
		request.set_StartTime(StartTime)
		request.set_EndTime(EndTime)

		response = self.client.do_action_with_exception(request)

		DBNodePerformanceDict = {}

		for PerformanceJson in (json.loads(response))['PerformanceKeys']['PerformanceItem']:
			Value = 0 
			for i in range(len(PerformanceJson['Points']['PerformanceItemValue'])):
				Value += float(PerformanceJson['Points']['PerformanceItemValue'][i]['Value'])
			DBNodePerformanceDict[PerformanceJson['MetricName']] = round(Value/len(PerformanceJson['Points']['PerformanceItemValue']), 2)
		
		return DBNodePerformanceDict
