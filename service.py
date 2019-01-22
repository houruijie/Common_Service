from sanic import Sanic
from sanic.response import json
# from sanic.log import logger
import configparser
import traceback
import logging
# import sys
from retrying import retry
# import asyncio
from multiprocessing import Process
# import aiohttp
import json as nomaljson
import requests
import time
import uuid
import kafka
from Handle_Message import Handle_Message
from Micro_Logger import deal_log


class Service(object):

    def __init__(self, config_path):

        self.handle = Handle_Message()

        # 日志输出 将级别为warning的输出到控制台，级别为debug及以上的输出到log.txt文件中
        logger = logging.getLogger("Service")
        logger.setLevel(logging.DEBUG)
        # 输出到文件
        file_handle = logging.FileHandler("./log.txt", mode='a')
        # file_handle.setLevel(logging.INFO)
        # 输出到控制台
        cmd_handle = logging.StreamHandler()
        # cmd_handle.setLevel(logging.INFO)
        # 输出的格式控制
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] %(message)s")
        file_handle.setFormatter(formatter)
        cmd_handle.setFormatter(formatter)
        # 添加两个输出句柄
        logger.addHandler(file_handle)
        logger.addHandler(cmd_handle)

        self.logger = logger

        # 定义钩子函数
        # 接收消息后的处理
        self._after_received = None
        self._before_request = None
        self._after_request = None
        self._before_finished = None
        self._handle_input_item = None
        self._health_check = None

        # 定义的变量
        self.app = Sanic()
        self.config_path = config_path
        try:
            cf = configparser.ConfigParser()
            cf.read(self.config_path)

            # 获取kafka集群信息
            servers = cf.options('kafka_cluster')
            kafka_cluster = []
            for item in servers:
                kafka_cluster.append(cf['kafka_cluster'][item])
            self.kafka_cluster = kafka_cluster

            # 获取配置文件中的服务信息
            self.service_ip = cf['service']['service_ip']
            self.service_port = cf.getint('service', 'service_port')
            self.service_name = cf['service']['service_name']
            self.service_type = cf['service']['service_type']
            self.healthcheck_path = cf['service']['healthcheck_path']

            # 默认的健康检查路径
            if len(self.healthcheck_path) == 0:
                self.healthcheck_path = "/health"

            self.service_meta = dict()
            if cf.has_section('service_meta'):
                metas = cf.options('service_meta')
                for item in metas:
                    self.service_meta[item] = cf['service_meta'][item]

            self.healthcheck_args = dict()
            if cf.has_section('healthcheck_args'):
                args = cf.options('healthcheck_args')
                for item in args:
                    self.healthcheck_args[item] = cf['healthcheck_args'][item]

            # 构造服务注册参数
            self.server_register_parameter = {
                "name": self.service_name,
                "type": self.service_type,
                "address": self.service_ip,
                "port": self.service_port,
                "meta": self.service_meta,
                "check": {
                    "args": self.healthcheck_args,
                    "path": self.healthcheck_path
                }
            }

            # 获取服务注册的url
            self.register_url = cf['register_service']['register_url']

            # 接收消息后反馈已接收信息url
            self.return_url = cf['return_message']['return_url']

        except Exception as e:
            self.logger.error(
                "Error ocurred when trying to read config file")
            raise

    # data-->request data预处理装饰器

    def before_request(self):
        def warrper(func):
            self._before_request = func
        return warrper

    # response --> data 处理数据提取
    def after_request(self):
        def wrapper(func):
            self._after_request = func
        return wrapper

    # 单条输入data的处理
    def handle_input_item(self):
        def wrapper(func):
            self._handle_input_item = func
        return wrapper

    # 健康检查的处理函数
    def health_check(self):
        def wrapper(func):
            self._health_check = func
        return wrapper

    # 结束之前进行的操作
    def before_finish(self):
        def wrapper(func):
            self._before_finish = func
        return wrapper

    # 收到消息之后的操作
    def after_received(self):
        def wrapper(func):
            self._after_received = func
        return wrapper

    def _retry_on_false(result):
        return result is False

    @retry(stop_max_attempt_number=3, retry_on_result=_retry_on_false, wait_fixed=2000)
    def send_message(self, mes, topic):
        try:
            mesg = str(nomaljson.dumps(mes)).encode('utf-8')
            producer = kafka.KafkaProducer(
                bootstrap_servers=self.kafka_cluster)
            producer.send(topic, mesg)
            self.logger.info("Send the message to next topic successully!")
            return True
        except Exception as err:
            return False
            raise
        finally:
            producer.close()

    # 依据data_list 和 config
    def deal_data_message(self, data_list, config):

        # data_list预处理
        # print("********************"+str(data_list))
        request_data = list(map(self._before_request, data_list))

        self.logger.info("the request_data_list:"+str(request_data))

        response_data = list(map(self._handle_input_item, request_data))
        print(response_data)

        self.logger.info("the response_data_list:"+str(response_data))

        result_list = list(map(self._before_request, response_data))
        
        self.logger.info("the result_data_list:"+str(result_list))

        return result_list

    # 消息解读函数

    def interpretate_message(self, message, message_type, serviceid, worktype, redis_ip="127.0.0.1", redis_port=6379):
        if message_type == 0:  # to control message handle
            self.handle.handle_control_message(message)
            return
        self.handle.initialize(serviceid, message, redis_ip, redis_port)
        output_flag = False  # if output_flag is true, stage finished, need to out put
        try:
            if self.handle.stage_position_judge():
                self.logger.info("judge is OK")
                output_flag = True
                if not self.handle.next_list and not self.handle.store_list:
                    self.handle.send_finished_message(self.handle.wokerid, self.handle.worker_type, 0, 0,
                                                      self.handle.taskid, self.handle.childid, "finished", self.handle.error_message)
                    return
                message['output']['depth'] = message['output']['depth'] + 1
        except Exception as e:
            # print("----------------------------"+str(self.handle.message))
            self.logger.error("something in message need")
            self.handle.error_message = self.handle.error_message + \
                ";" + str(e)
        if message['output']['depth'] >= message['output']['max_depth']:
            self.handle.send_finished_message(self.handle.wokerid, self.handle.worker_type, 0, 0, self.handle.taskid,
                                              self.handle.childid, "finished", self.handle.error_message)
            return
        # check config, decide use redis or not
        framework_config = self.handle.config.get('framework', None)
        if framework_config is None:
            redis_config = True
        else:
            redis_config = framework_config.get("redis", True)
        if redis_config is True:
            self.handle.info_list = self.handle.calculate_different_set(
                set(self.handle.input_list), self.handle.topic + "_" + self.handle.taskid)
        else:
            self.handle.info_list = self.handle.input_list
        if len(self.handle.info_list) <= 0:
            self.handle.send_finished_message(self.handle.wokerid, self.handle.worker_type, len(self.handle.info_list), 0, self.handle.taskid,
                                              self.handle.childid, "finished", self.handle.error_message)
            return
        result_list = self.deal_data_message(
            self.handle.info_list, self.handle.config.get("service", {}))
        try:
            # 处于stage的最后一个阶段，需要将数据输出到数据库和next指定的下一个stage的第一个微服务中
            if output_flag is True:
                try:
                    self.handle.store(self.handle.store_list,
                                      self.handle.info_list, result_list)
                except Exception as e:
                    self.l_group_id.error("the db error")
                    raise Exception("the db error")
                finished_flag = True
                for n in self.handle.next_list:  # next字段有值
                    finished_flag = False
                    self.handle.send_msg_kafka()
                if finished_flag is True:
                    self.handle.send_finished_message(self.handle.wokerid, self.handle.worker_type, len(self.handle.info_list), len(result_list),
                                                      self.handle.taskid, self.handle.childid, "finished", self.error_message)
                else:
                    self.handle.send_finished_message(self.handle.wokerid, self.handle.worker_type, len(self.handle.info_list), len(result_list),
                                                      self.handle.taskid, self.handle.childid, "running", self.handle.error_message)
                return
            # 不是微服务的最后一个阶段，需要将数据放到data中，通过kafka传递给下一个微服务
            else:
                message["data"] = result_list
                message["output"]["current_index"] = message["output"]["current_index"] + 1
                self.send_message(self.handle.message, self.handle.topic)
                self.handle.send_finished_message(self.handle.wokerid, self.handle.worker_type, len(self.handle.info_list), len(result_list),
                                                  self.handle.taskid, self.handle.childid, "running", self.handle.error_message)
                return
        except Exception as e:
            self.handle.error_message = self.handle.error_message + \
                ":" + str(e)
            traceback.print_exc()

        self.handle.insert_redis(
            set(self.handle.info_list), self.handle.topic + "_" + self.handle.taskid)

    # 对消息的完整性进行检验

    def message_check(self, message, message_type):
        if message_type == 1:
            try:
                if 'childid' not in message:
                    return (False, "the childid is missing")
                else:
                    childid = message.get('childid', None)
                    if type(childid) != int:
                        return (False, "childid must be int")

                if 'taskid' not in message:
                    return (False, "the taskid is missing")

                if 'data' not in message:
                    return (False, "the data missing")
                else:
                    data = message.get('data', None)
                    if type(data) != list:
                        return (False, "the data must be list")

                if 'output' not in message:
                    return (False, "the output is missing")
                else:
                    output = message.get('output', None)
                    if type(output) != dict:
                        return (False, "the output must be dict")
                    else:
                        if 'current_stage' not in output:
                            return (False, "the current_stage is missing")

                        if 'current_index' not in output:
                            return (False, "the current_index is missing")
                        else:
                            current_index = output['current_index']
                            if type(current_index) != int:
                                return (False, "the current_index must be int")

                        if 'depth' not in output:
                            return (False, "the depth is missing")
                        else:
                            depth = output['depth']
                            if type(depth) != int:
                                return (False, "the depth must be int")

                        if 'max_depth' not in output:
                            return (False, "the max_depth is missing")
                        else:
                            max_depth = output['max_depth']
                            if type(max_depth) != int:
                                return (False, "the max_depth must be int")

                        if 'stages' not in output:
                            return (False, "the stages is missing")
                        else:
                            stages = output['stages']
                            if type(stages) != dict:
                                return (False, "the stages must be dict")
                            else:
                                for key in stages.keys():
                                    if type(stages[key]) != dict:
                                        return (False, "stage in stages must be dict")
                                    else:
                                        temp = stages[key]

                                        if 'microservices' not in temp:
                                            return (False, "the microservices is missing")

                                        if 'next' not in temp:
                                            return (False, "the next is missing")

                                        if 'store' not in temp:
                                            return (False, "the store is missing")

                return (True, "the message is right")

            except Exception as err:
                self.logger.error(
                    "Some errors occored in the message:   "+traceback.format_exc())
                return (False, "Some errors occored in checking the message")

        else:
            # 预留控制字段信息的检查
            return (False, "control type is not support now")

    @retry(stop_max_attempt_number=3, retry_on_result=_retry_on_false, wait_fixed=2000)
    def send_finish_message(self, message, info):
        if 'taskid' not in message:
            temp_taskid = None
        else:
            temp_taskid = message['taskid']

        if 'childid' not in message:
            temp_childid = -1
        else:
            temp_childid = message['childid']

        send_message = {
            "type": "received",
            "workerid": self.service_id,
            "worker_type": self.service_type,
            "valid_input_length": 0,
            "output_length": 0,
            "taskid": temp_taskid,
            "childid": temp_childid,
            "status": "finished",
            "error_msg": info
        }
        parametas = nomaljson.dumps(send_message)
        try:
            ret = requests.put(self.return_url, params=parametas, timeout=2)
            temp = ret.json()
            if temp['state'] == 0:
                self.logger.info("task finished")
                return True
            else:
                self.logger.error(
                    "the parameters of sending finished message is wrong")
                return False
        except Exception as err:
            self.logger.error("Errors occored while sending finished message")
            return False
            raise

    @retry(stop_max_attempt_number=3, retry_on_result=_retry_on_false, wait_fixed=2000)
    def send_received_message(self, message):
        if 'taskid' not in message:
            temp_taskid = None
        else:
            temp_taskid = message['taskid']

        if 'childid' not in message:
            temp_childid = -1
        else:
            temp_childid = message['childid']

        send_message = {
            "type": "received",
            "workerid": self.service_id,
            "worker_type": self.service_type,
            "taskid": temp_taskid,
            "childid": temp_childid,
            "task_message": message
        }
        parametas = nomaljson.dumps(send_message)
        try:
            ret = requests.put(self.return_url, params=parametas, timeout=2)
            temp = ret.json()
            if temp['state'] == 0 and temp['status'] == "running":
                self.logger.info("the task need to be done")
                return True
            else:
                return False
        except Exception as err:
            self.logger.error(
                "Errors occored while sending received message:  "+traceback.format_exc())
            return False
            raise

    # 监听一条消息
    def listen_one_message(self, topic, group_id):
        try:
            temp_result = None
            consumer = kafka.KafkaConsumer(
                group_id=group_id, bootstrap_servers=self.kafka_cluster)
            consumer.subscribe(topics=(topic))
            message = consumer.poll(timeout_ms=5, max_records=1)
            if len(message) != 0:
                for key in message.keys():
                    message = nomaljson.loads(
                        message[key][0].value.decode('utf-8'))
                    if group_id != self.c_group_id:
                        self.logger.info("gain one message in "+str(group_id))
                    else:
                        self.logger.info("gain one message in c_group")
                temp_result = message
            else:
                if group_id != self.c_group_id:
                    self.logger.info("no message get in "+str(group_id))
                else:
                    self.logger.info("no message get in c_group")
        except Exception as err:
            self.logger.error(
                "Error meet during get message:  "+traceback.format_exc())
            raise
        finally:
            consumer.close()
            return temp_result

    # kafka消息获取

    def listen_message(self):
        mes_sign = [{
            "topic": self.service_controltopic,
            "message_type": 0,
            "group_id": self.c_group_id
        }, {
            "topic": self.service_hightopic,
            "message_type": 1,
            "group_id": self.h_group_id
        }, {
            "topic": self.service_lowertopic,
            "message_type": 1,
            "group_id": self.l_group_id
        }]
        while True:
            for i in range(0, 3):
                message = self.listen_one_message(
                    mes_sign[i]['topic'], mes_sign[i]['group_id'])
                try:
                    if message != None:
                        # print(type(message))
                        # print(message)

                        self.logger.info("the message receive: "+str(message))

                        self.logger.info("sending receiving message")
                        if self.send_received_message(message):
                            self.logger.info("checing the received message")
                            if self.message_check(message, mes_sign[i]['message_type'])[0]:
                                # 之后这边是调用侯的代码
                                self.interpretate_message(
                                    message, mes_sign[i]['message_type'], self.service_id, self.service_type)
                            else:
                                info = self.message_check(
                                    message, mes_sign[i]['message_type'])[1]
                                self.logger.warning(
                                    "errors occored while checking the message: "+info)
                                if mes_sign[i]['message_type'] == 1:
                                    self.send_finish_message(message, info)
                        else:
                            self.logger.error(
                                "parameter missing or error in sending received message")
                        break
                except Exception as err:
                    raise
            time.sleep(1)

    # 同步服务注册
    # 如果返回false  重试3次

    @retry(stop_max_attempt_number=3, retry_on_result=_retry_on_false, wait_fixed=2000)
    def resigter_service(self):
        parametas = nomaljson.dumps(self.server_register_parameter)
        try:
            # 设置的超时时间为两秒
            ret = requests.post(self.register_url, params=parametas, timeout=2)
            temp = ret.json()
            self.service_id = temp['id']
            # print(self.service_id)
            self.service_lowertopic = temp['topic']['low_priority']
            self.service_hightopic = temp['topic']['high_priority']
            self.service_controltopic = temp['topic']['controller']
            # 控制消息每次微服务启动时都不一致保证所有微服务都能接收到消息
            self.c_group_id = str(uuid.uuid1())
            self.h_group_id = "h_group"  # 高优先级group
            self.l_group_id = "l_group"  # 低优先级group
            self.service_state = temp['state']
            # print(type(self.service_state))
            # print(self.service_state)
            if self.service_state is True:
                self.logger.info('register service success!')
                return True
            else:
                self.logger.error(
                    'service register fail with the return of service manager')
                return False
        except Exception:
            self.logger.error('service register fail   ' +
                              traceback.format_exc())
            raise
            return False

    # 默认的健康检查信息
    async def default_health_check(self, request):
        return json({
            "state": "health",
            "info": "service is healthy"
        })

    # 开启sanic
    def start_sanic(self):
        try:
            if self._health_check != None:
                self.app.add_route(self._health_check,
                                   uri=self.healthcheck_path)
            else:
                self.logger.warning("using default health check function")
                self.app.add_route(self.default_health_check,
                                   uri=self.healthcheck_path)
            # 添加健康检查的路由
            self.app.run(self.service_ip, self.service_port)
        except Exception as e:
            self.logger.error(
                "Error occored during starting sanic: "+traceback.format_exc())
            raise

    # 服务运行
    def run(self):
        try:
                # 函数预处理是否已经写好

                # 函数执行是否已经写好

                # 函数获取的数据提取是否已经写好

                # 新的线程 利用sanic监听健康检查
            # self.p = Process(target=self.start_sanic)
            # self.p.start()
            temp = self.resigter_service()
            # 注册服务,重试的次数最大为3次，返回true才算成功
            if temp:
                self.listen_message()
            else:
                self.logger.error(
                    "Can't register the service after retrying three times")

        except Exception as err:
            self.logger.error(
                "Error occored while running the main process:  "+traceback.format_exc())
            raise
