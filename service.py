from sanic import Sanic
from sanic.response import json
from sanic.log import logger
import configparser
import traceback
import logging
import sys
import tool
from retrying import retry
import asyncio
from multiprocessing import Process
import aiohttp
import json as nomaljson
import requests
import time
import uuid
import kafka

class Service(object):
    
    def __init__(self, config_path):
        
        #日志输出 将级别为warning的输出到控制台，级别为debug及以上的输出到log.txt文件中
        self.logger = logging.getLogger("Service")
        #输出到文件
        file_handle=logging.FileHandler("./log.txt",mode='a')
        file_handle.setLevel(logging.NOTSET)
        #输出到控制台
        cmd_handle=logging.StreamHandler()
        cmd_handle.setLevel(logging.NOTSET)
        #输出的格式控制
        formatter = logging.Formatter("[%(asctime)s]--%(levelname)s: %(message)s")
        file_handle.setFormatter(formatter)
        cmd_handle.setFormatter(formatter)
        #添加两个输出句柄
        self.logger.addHandler(file_handle)
        self.logger.addHandler(cmd_handle)

        #定义钩子函数

        self.before_request=None
        self.after_request=None


        #定义的变量
        self.app=Sanic()
        self.config_path=config_path
        try:
            cf = configparser.ConfigParser()
            cf.read(self.config_path)
            
            #获取kafka集群信息
            servers = cf.options('kafka_cluster')
            kafka_cluster=[]
            for item in servers:
                kafka_cluster.append(cf['kafka_cluster'][item])
            self.kafka_cluster=kafka_cluster
            # print(type(kafka_cluster))
            # print(kafka_cluster)

            #获取配置文件中的服务信息
            self.service_ip=cf['service']['service_ip']
            self.service_port=cf.getint('service','service_port')
            self.service_name=cf['service']['service_name']
            self.service_type=cf['service']['service_type']
            self.healthcheck_path=cf['service']['healthcheck_path']
            
            self.service_meta=dict()
            if cf.has_section('service_meta'):  
                metas=cf.options('service_meta')
                for item in metas:
                    self.service_meta[item]=cf['service_meta'][item]
            
            self.healthcheck_args=dict()
            if cf.has_section('healthcheck_args'):
                args=cf.options('healthcheck_args')
                for item in args:
                    self.healthcheck_args[item]=cf['healthcheck_args'][item]
            
            #构造服务注册参数
            self.server_register_parameter={
                "name":self.service_name,
                "type":self.service_type,
                "address":self.service_ip,
                "port":self.service_port,
                "meta":self.service_meta,
                "check":{
                    "args":self.healthcheck_args,
                    "path":self.healthcheck_path
                }
            }

            #获取服务注册的url
            self.register_url=cf['register_service']['register_url']

        except Exception as e:
            self.logger.exception("Error ocurred when trying to read config file")
            raise
    
    
    
    

    

    def _retry_on_false(result):
        return result is False


    
    
    #消息解读
    def interpretate_message(message,message_type):
        print("------------------")

    #对消息的完整性进行检验
    def message_check(message,message_type):
        return True

    def send_finish_message(*args,**kwargs) :
        print("send finish message")
    
    def send_received_message(*args,**kwargs) :
        print("send received message")
    
    
    #监听一条消息
    def listen_one_message(self,topic,group_id):
        try:
            temp_result=None
            consumer = kafka.KafkaConsumer(group_id=group_id,bootstrap_servers=self.kafka_cluster)
            consumer.subscribe(topics=(topic))
            message=consumer.poll(timeout_ms=5,max_records=1)
            if len(message)!= 0:
                for key in message.keys():
                    message=nomaljson.loads(message[key][0].value.decode('utf-8'))
                    logger.info("gain one message in "+str(group_id))
                temp_result=message
            else:
                logger.info("no message get in "+str(group_id))
        except Exception as err:
            self.logger.error("Error meet during get message:  "+traceback.format_exc())
            raise
        finally:
            consumer.close()
            return temp_result



    #kafka消息获取
    def listen_message(self):
        mes_sign=[{
            "topic":self.service_controltopic,
            "message_type":0,
            "group_id":self.c_group_id
        },{
            "topic":self.service_hightopic,
            "message_type":1,
            "group_id":self.h_group_id
        },{
            "topic":self.service_lowertopic,
            "message_type":1,
            "group_id":self.l_group_id
        }]
        while True:
            for i in range(0,3):
                message=self.listen_one_message(mes_sign[i]['topic'],mes_sign[i]['group_id'])
                if message!=None:
                    print(message)
                    # self.send_received_message()
                    # if self.message_check(message,mes_sign[i]['message_type']):
                    #     #之后这边是调用侯的代码
                    #     self.interpretate_message(message,mes_sign[i]['message_type'])
                    break
            time.sleep(1)

    #同步服务注册
    #如果返回false  重试3次
    @retry(stop_max_attempt_number=3,retry_on_result=_retry_on_false,wait_fixed=2000)
    def resigter_service(self):
        parametas=nomaljson.dumps(self.server_register_parameter)
        try:
            #设置的超时时间为两秒
            ret=requests.post(self.register_url,params=parametas,timeout=2)
            temp=ret.json()
            self.service_id=temp['id']
            self.service_lowertopic=temp['topic']['low_priority']
            self.service_hightopic=temp['topic']['high_priority']
            self.service_controltopic=temp['topic']['controller']
            self.c_group_id=str(uuid.uuid1())#控制消息每次微服务启动时都不一致保证所有微服务都能接收到消息
            self.h_group_id="h_group"  #高优先级group
            self.l_group_id="l_group"  #低优先级group
            self.service_state=temp['state']
            print(type(self.service_state))
            print(self.service_state)
            if self.service_state is True:
                logger.info('register service success!')
                return True
            else:
                self.logger.error('service register fail with the return of service manager')
                return False
        except Exception:
            self.logger.error('service register fail   '+traceback.format_exc())
            raise
            return False

    #健康检查的装饰器
    async def health_check_decorator(self,request):

        return json({
            "status":"health",
            "infor":"wwwww"
        })

    #开启sanic
    def start_sanic(self):
        try:
            self.app.add_route(self.health_check_decorator,uri=self.healthcheck_path)
            #添加健康检查的路由
            self.app.run(self.service_ip,self.service_port)
        except Exception as e:
            self.logger.exception("Error occored during starting sanic")
            raise
    
    #服务运行
    def run(self):
        #健康检查的函数是否已经写好
        
        #函数预处理是否已经写好

        #函数执行是否已经写好

        #函数获取的数据提取是否已经写好

        try:
            #新的线程 利用sanic监听健康检查
            # self.p=Process(target=self.start_sanic)
            # self.p.start()
        
            # temp=self.resigter_service()
            data=["121212"]
            config=None
            self.before_request(data,config)
            
            # self.predeal(data,config)
        
            # #注册服务,重试的次数最大为3次，返回true才算成功
            # if temp:
            #     self.listen_message()
            # else:
            #     self.logger("Can't register the service after retrying three times")

        except Exception as err:
            raise        

    
    #data-->request data预处理装饰器
    def register_before_request(self,func):
        self.before_request=func




    # def predeal_data(self):
    #     def inner_func(func):
    #         def wrapper(*args,**kwargs):
    #             print("111111111")
    #             self.predeal=func
    #             # print(func.__name__)
    #             ret=func(*args,**kwargs)
    #             return ret
    #         return wrapper
    #     return inner_func



    

    #


