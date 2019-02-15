from sanic.response import json as sjson
import redis
from Database_Handle import MongoDB_Store
from Database_Handle import MySql_Store
from Micro_Logger import deal_log
import traceback
import asyncio
import requests
import json
import time
from redis_handle import calculate_set
from redis_handle import insert

class Handle_Message():
    def __init__(self):
        self.error_info = {}
        self.finished_url = "http://mock.eolinker.com/BuQt7kT3de7f2186783e1777a29045300b7b9b29cc0a49c?uri=/worker"

    #函数功能：完成必要变量的初始化
    def initialize(self,serverid,message,worker_type,redis_ip="127.0.0.1",redis_port=6379):
        self.redis_handle = redis.Redis(host=redis_ip,port=redis_port)
        self.wokerid =serverid
        self.worker_type = worker_type
        self.message = message
        self.stage = message['output']['current_stage']
        self.index = message['output']['current_index']
        self.next_list = message['output']['stages'][self.stage]['next']
        self.store_list = message['output']['stages'][self.stage]['store']
        self.taskid = message["taskid"]
        self.childid = message["childid"]
        self.input_list = message['data']
        self.topic = message['output']["stages"][self.stage]["microservices"][self.index]["topic"]
        self.config = message['output']["stages"][self.stage]["microservices"][self.index]["config"]

    # 判断所处的stage的位置
    # def stage_position_judge(self,message):
    #     stage=message['output']['current_stage']
    #     index = message['output']['current_index']
    #     server_list = message['output']['stages'][stage]['microservices']
    #     length = len(server_list)
    #     if index+1 >= length:
    #         return True
    #     return False
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
    def send(self, m):
        resp = requests.put(self.finished_url, params=json.dumps(m), timeout=3)
        response_dic = resp.json()
        return response_dic


    # send finshed message and error message to the finished_api
    def send_finished_message(self, workerid, worker_type, valid_input_length, output_length,
                              taskid, childid, status,error_info):
        m = {
            "type": "finished",
            "wokerid": workerid,
            "worker_type":worker_type,
            "valid_input_length":valid_input_length,
            "output_length":output_length,
            "taskid": taskid,
            "childid": childid,
            "status": status,
            "error_msg": error_info
        }
        print(m)
        # if can not connect to the finished API, retry 3 times
        try:
            response_dic = self.send(m)
        except Exception as e:
            err = "Error 111: the Api which received finished message can not reached"
            deal_log(err, "error")
            self.error_info["finished_error"] = err
            return
        # get response text. 0:success, -2:para is wrong
        print(response_dic)
        if not response_dic.get("state", -2) == 0:
            deal_log("Error: exception occur in send_finished_message function. the url or json data is wrong")
        else:
            print("send finished message success")

    @retry(stop_max_attempt_number=4, wait_exponential_multiplier=1000, wait_exponential_max=10000)
    def get_sub(self, redis_handle, set_name, info_set, reids_ip, reids_port):
        try:
            redis_handle.delete("set_help")
            pipe = redis_handle.pipeline(transaction=False)
            # ་将数据存放到set_help,使用pipeline
            for value in info_set:
                pipe.sadd("set_help", value)
            pipe.execute()
            # 将set_help和历史记录做差
            sub = redis_handle.sdiff("set_help", set_name)
            # 返回set_help中的数据
            return sub
        except Exception as e:
            redis_handle = redis.Redis(host=reids_ip, port=reids_port, decode_responses=True)
            pipe = redis_handle.pipeline(transaction=False)
            raise
    # 函数功能：将输入数据和历史数据作差集
    # 输入：set1:输入集合，set_name:集合名字，redis_?：链接信息
    # 输出：与历史记录的差集
    def calculate_different_set(self, set1, set_name, redis_ip='127.0.0.1', reids_port=6379):
        r_list = list()
        try:
            r_list = list(self.get_sub(self.red, set_name, set1, redis_ip, reids_port))
        except Exception as e:
            r_list = list(set1)
            deal_log('Error 111: Connection to the redis refused in calculate_different_set', "error")
            self.error_info["redis_error1"] = str(e)
        finally:
            return r_list

    @retry(stop_max_attempt_number = 3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
    def add_info_list( self, redis_handle, set_name, info_set ):
        # 将set1中的数据添加到历史数据的set集合中
        try:
            redis_handle.sadd(set_name, "set_help")
        except Exception as e:
            redis_handle = redis.Redis(host=reids_ip, port=reids_port, decode_responses=True)
            raise
    # 函数功能：将数据插入到redis中
    # 输入：set1:输入集合，set_name:集合名字，redis_?：链接信息
    # 输出：将数据插入到redis.set()中
    def insert_redis(self, set1, set_name, redis_ip='127.0.0.1', reids_port=6379):

        try:
            self.add_info_list(self.red, set_name, set1)
        except Exception as e:
            deal_log(e,"error")
            self.error_info["redis_error1"] = str(e)

    # send message to kafka
    def send_msg_kafka(self):
        try:
            print(self.topic)
            print(self.message)
            print("you should send message to kafka")
        except Exception as e:
            self.error_message = self.error_message+";"+str(e)

    def store(self, store_list, data_list, result_list):
        for s_way in store_list:
            if s_way["type"] == "mongoDB":
                url = s_way["url"]
                db_name = s_way["database"]
                col_name = s_way["collection"]
                mongodb = MongoDB_Store()
                try:
                    mongodb.store_data_list(url, db_name, col_name, data_list, result_list)
                except Exception as e:
                    deal_log(e, "error")
                    self.error_info["redis_error1"] = str(e)

            elif s_way["type"] == "mysql":
                i = 0
                while i < len(data_list):
                    sql = "insert into table(key,vlaue) values('{key}','{value}')".format(key=str(data_list[i]),
                                                                                          value=str(result_list[i]))
                    MySql_Store().insert(sql)
                    i = i + 1
                pass
            elif s_way["type"] == "file":
                pass
            else:
                pass


    # def interpretate_message(self,message,message_type,serviceid,worker_type,redis_ip="127.0.0.1",redis_port=6379):
    #     if message_type == 0:     # to control message handle
    #         self.handle_control_message(message)
    #         return
    #     self.initialize(serviceid, message, worker_type,redis_ip, redis_port)
    #     output_flag = False  # if output_flag is true, stage finished, need to out put
    #     try:
    #         if self.stage_position_judge():
    #             output_flag = True
    #             if not self.next_list and not self.store_list:
    #                 self.send_finished_message(self.wokerid, self.worker_type, 0, 0, self.taskid,
    #                                            self.childid, "finished", self.error_message)
    #                 return
    #             message['output']['depth'] = message['output']['depth'] + 1
    #     except Exception as e:
    #         deal_log("Error: something in message need")
    #         self.error_message = self.error_message + ";" +str(e)
    #     if message['output']['depth'] >= message['output']['max_depth']:
    #         self.send_finished_message(self.wokerid, self.worker_type, 0, 0, self.taskid,
    #                                    self.childid, "finished", self.error_message)
    #         return
    #     # check config, decide use redis or not
    #     framework_config = self.config.get('framework',None)
    #     if framework_config == None:
    #         redis_config = True
    #     else:
    #         redis_config = framework_config.get("redis",True)
    #     if redis_config == True:
    #         self.info_list = self.calculate_different_set(set(self.input_list), self.topic+"_"+self.taskid)
    #     else:
    #         self.info_list = self.input_list
    #     if len(self.info_list) <= 0:
    #         self.send_finished_message(self.wokerid, self.worker_type, len(self.info_list), 0, self.taskid,
    #                                    self.childid, "finished", self.error_message)
    #         return
    #     result_list = self.main_funciton_interface()
    #     try:
    #         #处于stage的最后一个阶段，需要将数据输出到数据库和next指定的下一个stage的第一个微服务中
    #         if output_flag == True:
    #             try:
    #                 self.store(self.store_list, self.info_list,result_list)
    #             except:
    #                 deal_log("the db error","error")
    #                 raise Exception("the db error")
    #             finished_flag = True
    #             for n in self.next_list:  # next字段有值
    #                 finished_flag = False
    #                 self.send_msg_kafka()
    #             if finished_flag == True:
    #                 self.send_finished_message(self.wokerid, self.worker_type, len(self.info_list), len(result_list), self.taskid,
    #                                    self.childid, "finished", self.error_message)
    #             else:
    #                 self.send_finished_message(self.wokerid, self.worker_type, len(self.info_list), len(result_list), self.taskid,
    #                                    self.childid, "running", self.error_message)
    #             return
    #         #不是微服务的最后一个阶段，需要将数据放到data中，通过kafka传递给下一个微服务
    #         else:
    #             message["data"] = result_list
    #             message["output"]["current_index"] = message["output"]["current_index"] + 1
    #             self.send_msg_kafka()
    #             self.send_finished_message(self.wokerid, self.worker_type, len(self.info_list), len(result_list), self.taskid,
    #                                    self.childid, "running", self.error_message)
    #             return
    #     except Exception as e:
    #         self.error_message = self.error_message+":"+str(e)
    #         traceback.print_exc()

    #     self.insert_redis(set(self.info_list), self.topic+"_"+self.taskid)


    #函数功能:对控制消息进行处理
    def handle_control_message(self,message):
        print("this is handle_control_message")


# msg = {
#         "taskid": "add23d23d23d",
#         "childid": 2,
#         "config": {},
#         "data": [31,32,33,34],
#         "output": {
#             "current_stage": "stageA",
#             "current_index": 1,
#             "depth": 1,
#             "max_depth": 10,
#             "stages": {
#                 "stageA": {
#                     "microservices": [{"topic": "Fibonacci", "config": {"framework":{"redis":False},"serivce": {"concurrency":15}}},
#                             {"topic": "Fibonacci", "config": {"serivce": {"concurrency":15}}}
#                           ],
#                     "next": [],
#                     "store": [{
#                         "type": "mongoDB",
#                         "url": "mongodb://root:123456@127.0.0.1:27017",
#                         "database": "testdb",
#                         "collection": "testcln"
#                     }]
#                 },
#                 "stageB": {
#                     "ms": [{"topic": "DNSTopic", "config": {}}, {"topic": "CollectorTopic", "config": {}}],
#                     "next": [],
#                     "store": [{
#                         "type": "mongoDB",
#                         "url": "mongodb://root:123456@127.0.0.1:27017",
#                         "database": "testdb",
#                         "collection": "testcln2"
#                     }]
#                 }
#             }
#         }
#     }
# control_mesg = {
#         "type":"stop",
#         "taskid":["add23d23d23d","add23d23d23e","add23d23d23p"]
#     }
# handle = Handle_Message()
# handle.interpretate_message(msg,1,serviceid=1212121)