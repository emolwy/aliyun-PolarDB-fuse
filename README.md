#### 阿里云PolarDB数据库-慢SQL自动查杀(基于Python3.7) 
基于Python3.7，定时抓取PolarDB资源使用情况，如资源出现负载过高，则自动查杀PolarDB集群中的慢SQL。 

#### 1、安装Python扩展 
````bash
pip install -r requirement.txt
````

#### 2、配置app.config 
````ini
[CONFIG]
# 配置阿里云AcceessKey 用于获取Polardb集群信息。
accessKeyId = xxxxxx
accessSecret = xxxxxx
regionId = cn-hangzhou

# 配置Redis连接信息
REDIS_HOST = 127.0.0.1
REDIS_PORT = 6379
REDIS_PASS = PASSWORD

# PolarDB数据库普通账号，只创建，不授权访问任何业务数据库，用于执行SHOW PROCESSLIST语句。
DB_USERNAME = YOURUSERNAME
DB_PASSWORD = YOURPASSWORD

#PolarDB的规格参数,参考阿里云官网
medium = {'Max_Connection': 1200, 'Max_IOPS': 8000}
large = {'Max_Connection': 5000, 'Max_IOPS': 32000}
xlarge = {'Max_Connection': 10000, 'Max_IOPS': 64000}

# 慢SQL定义：执行时长超过15秒
SLOWSQL_QUERY_TIME = 15

# 慢SQL的数量：执行时长超过15秒的数据库进程数
SLOWSQL_CONNECTION_COUNT = 1

# PolarDB告警指标定义：连接数使用率、IOPS和CPU使用率，单位百分比% 。
CONNECTION_USAGE = 60
IOPS_USAGE = 80
CPU_USAGE = 90

# 企业微信Webhook Robot 慢SQL自动查杀通知
WXWORK_ROBOT_KEY = XXXXXX-a791-47e1-af62-bbXXXXXX71e
WXWORK_USER_LIST = ["USER1", "USER2", ......, "@all"]
````

#### 3、编辑syncPolarDBClusterInfo.py，配置完字典UsersDict，并执行。 
````python

# 数据库常用账号及密码已字典格式写入Redis
# 格式：{'USERNAME1': 'PASSWORD1', 'USERNAME2': 'PASSWORD2', ......}
UsersDict = {'USERNAME1': 'PASSWORD1', 'USERNAME2': 'PASSWORD2', ......}
conn.set("UsersDict", json.dumps(UsersDict))

````

#### 4、设置日志路径
````python
# 编辑aliyun-polardb-fuse.py, 调整日志保存路径
logging.basicConfig(filename='/aliyun-polardb-fuse/logs/system.log', level=logging.DEBUG, format='%(asctime)s - [%(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
````

#### 5、配置定时任务 
````bash
0-59/2 7-22 * * * python3 aliyun-polardb-fuse.py #定期巡检，资源异常及存在慢SQL，则自动查杀。
0 0-23/12 * * * python3 syncPolardbClusterInfo.py #定期更新集群信息
````

#### 6、查看运行日志 
````bash
tail -f ./logs/system.log
````