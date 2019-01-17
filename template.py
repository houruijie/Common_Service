from sanic.response import json as sjson
from sanic import Sanic
from sanic.log import logger
import asyncio
import kafka
import json
import consul
import aiohttp
import time
import os
import traceback

#从环境中获取环境变量
#API相关
# request_url=os.environ['request_url']
# request_type=os.environ['post'] #请求的方式
# paras=os.environ['paras'] #请求所需要的参数
# para_usetype=os.environ['para_usetypr'] #参数携带的方式  1.URL中携带 2.请求体中携带 3.其他方式携带


# #接受kafka消息相关
kafka_cluster=os.environ['kafka_cluster']
# receive_topic=os.environ['receive_topic']
# consume_id=os.environ['consume_id']

#服务注册和健康度检查相关
server_ip=os.environ['server_ip']
server_port=os.environ['server_port']
server_name=os.environ['server_name']
server_type=os.environ['server_type']
server_meta=os.environ['server_meta']
heathcheck_args=os.environ['heathcheck_args']
heathcheck_path=os.environ['heathcheck_path']


global server_id
global server_controltopic
global server_hightopic
global server_lowertopic
global server_state


#kafka集群信息
mes_kafka=['127.0.0.1:9092']
mes_topic="test"

#consul集群消息
cs=consul.Consul(host='127.0.0.1',port=8500)


history_mes=[]





# async def get_deal_mes():
#     consumer=kafka.KafkaConsumer(mes_topic,group_id="domain_to_ip",bootstrap_servers=mes_kafka)
#     #阻塞直到获取kafka消息
#     for item in consumer:
#         mes=json.loads(item.value.decode('UTF-8'))
#         print("message get:"+str(mes))
#         # {
#         #     "api_par":{
#         #         "domains":{
#         #             "baidu.com",
#         #             "tencent.com",
#         #             "sohu.com"
#         #         }
#         #     },
#         #     "datadeal_par":{
#         #         "save_type":"0",#0 topic #1 mongodb #2 redis #3 es
#         #         "back_topic":"testtopic",
#         #         "Ip_Port":""
#         #     }
#         # }
        
#         #通过API获取相应的数据
#         if mes['api_par'] is not None:
#             response=await data_get(mes['api_par'])
#         else:
#             continue

#         await data_deal(response,mes['datadeal_par'])


# async def data_get(api_par):
#     #DNS正向解析
#     ip_result=dict()
#     for domain in api_par['domains']:
#         ips=set()
#         async with aiohttp.ClientSession() as session:
#             async with session.post('http://10.245.146.42:7777/batch?domain='+domain+'&type=A&mode=10') as r:
#                 t=json.loads(await r.text())
#                 for item in t['results'][0]['A']:
#                     for item1 in item['answer list']:
#                         ips.add(item1['data'])
#                 ip_result[domain]=list(ips)
#     temp=json.dumps(ip_result)
#     return temp

#斐波那契实现
async def fbnq(num):
    if num ==1:
        return 1
    elif num==2:
        return 1
    else:
        a=1
        b=1
        while num > 2:
            c=a
            a=b
            b=c+b
            num=num-1
        return b

app=Sanic()

# async def deal_mes(mes):
#     if len(mes) != 0:

#监听消息及消息优先级处理


    # for item in consumer:
    #     mes=json.loads(item.value.decode('UTF-8'))
    #     print("message get:"+str(mes))
        
    #     #对消息进行处理
    #     await deal_mes(mes)

#控制处理





@app.listener('after_server_start')
async def register_service(app, loop):
    #服务注册
    #检查必要的服务信息
    if len(server_name)!=0 and len(server_type) !=0 :
        para={
            "name":server_name,
            "type":server_type,
            "address":server_ip,
            "port":server_port,
            "meta":{},
            "check":{
                "args":heathcheck_args,
                "path":heathcheck_path
            }
        }

        paras=json.dumps(para)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post('http://result.eolinker.com/BuQt7kT3de7f2186783e1777a29045300b7b9b29cc0a49c?uri=/service/register',params=paras) as r:
                    temp=json.loads(await r.text())
                    server_id=temp['id']
                    server_lowertopic=temp['topic']['low_priority']
                    server_hightopic=temp['topic']['high_priority']
                    server_controltopic=temp['topic']['controller']
                    server_state=temp['state']

                    if server_state:
                        logger.info('register service success!  '+'server_id:'+server_id+'server_topic:'+server_lowertopic)
                    else:
                        logger.error('register service fail! Please check the parameters')
                        app.stop()
        except Exception:
            logger.error('register service fail! Please check the parameters')
            traceback.print_exc()
            app.stop()
    else:
        logger.error('lack the parameter(server_name or server_type) of server to register server !')
        app.stop()

def mes_get():
    control_consumer=kafka.KafkaConsumer(server_controltopic,bootstrap_servers=kafka_cluster)
    lower_consumer=kafka.KafkaConsumer(server_lowertopic,bootstrap_servers=kafka_cluster)
    hight_consumer=kafka.KafkaConsumer(server_hightopic,bootstrap_servers=kafka_cluster)


@app.get(heathcheck_path)
async def health_check(request):
    return sjson({
        "state":0,
        "infor":"health"
    })


if __name__ == "__main__":
    if len(server_ip) != 0 and len(server_port) !=0:
        app.run(server_ip,server_port)
    else:
        logger.info('start server fail,lack ip or port')
