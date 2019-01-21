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
# from message_distinguish import message_distinguish as mes_deal
import uuid
import redis

#从环境中获取环境变量
#API相关
# request_url=os.environ['request_url']
# request_type=os.environ['post'] #请求的方式
# paras=os.environ['paras'] #请求所需要的参数
# para_usetype=os.environ['para_usetypr'] #参数携带的方式  1.URL中携带 2.请求体中携带 3.其他方式携带


# #接受kafka消息相关
kafka_cluster=["127.0.0.1:9092"]
h_group_id="h_group"
l_group_id="l_group"
control_group_id=str(uuid.uuid1())
server_controltopic="t1"
server_hightopic="t2"
server_lowertopic="t3"
global server_id
global server_state

#服务注册和健康度检查相关
server_ip="127.0.0.1"
server_port="3000"
server_name="test"
server_type="FBNQ"
# server_meta=os.environ['server_meta']
# heathcheck_args=os.environ['heathcheck_args']
heathcheck_path="/health"
heathcheck_args={
    #kafka健康检查
    "est_num":4
}
#存储任务的redis数据库信息
redis_ip="127.0.0.1"
redis_port=6379

stop_task=set()

#将任务执行情况返回给控制处
def to_control(mes,info):
    logger.info=info
    print("mes send to control:"+mes)

#将非空的结果返回给下一消费者群
def to_next(mes,topic):
    sendmes(mes,topic)

#将结果存入数据库
def to_db(store_info,configs):
    print("存入数据库")


#适配API的一些其他参数
def suit_handle(mes):
    return mes
    print("data deal before handle")


#API获取数据之后的处理函数
def getdata_deal(mes):
    return mes
    print("data deal after get")

#消息中data的单条数据处理
async def mes_deal(request):
    if request ==1:
        return 1
    elif request==2:
        return 1
    else:
        a=1
        b=1
        while request > 2:
            c=a
            a=b
            b=c+b
            request=request-1
        return b



#依据配置文件微服务执行的速度-->将数据转为API所需要的格式-->处理数据-->获取的数据转为data中的格式
def mes_predeal(congfig,data):
    
    print("deal task")
    #依据配置文件处理data


#解读消息并进行任务数据去重
def mes_analysis(mes):
    #判断消息数据是否为空
    if len(mes['data'])==0:
        to_control({
            "info":"init task fail!"
        },"task data is null")
    else:

        #判断是否超出了max_depth
        if mes['output']['depth']>mes['output']['max_depth']:
            to_control({
                "type":"finished",
                "workerid":server_id,
                "taskid":mes['taskid'],
                "child_id":mes['childid'],
                "task_finished":True,
                "finish_info":"rearch the max depth",
                "error":False,
                "err_msg":""
            },"rearch the max depth")
            return
        else:
            pass
        
        #依据taskid获取此微服务之前执行的任务列表，去除执行过的
        data=set(mes['data'])
        try:
            rd=redis.Redis(host=redis_ip,port=redis_port)
            #获取set的已有集合长度
            temp_len=rd.scard(mes['taskid'])
            temp=set()
            #除去之前执行过的data
            if temp_len!=0:
                temp=rd.smembers(mes['taskid'])
                data=data-temp
            else:
                pass

            if len(data)!=0:
                #读取配置文件执行任务
                current_stage=mes['output']['current_stage']
                current_index=mes['output']['current_index']
                config=mes['output']['stages'][current_stage]['microservices'][current_index]['config']
                
                res=mes_deal(config,data)
                #返回结果为空
                if len(res)==0:
                    to_control({
                        "type":"finished",
                        "workerid":server_id,
                        "taskid":mes['taskid'],
                        "child_id":mes['childid'],
                        "task_finished":True,
                        "finish_info":"find esult is null",
                        "error":False,
                        "err_msg":""
                    },"find result is null")
                    return
                else:

                    ms_len=len(mes['output']['stages'][current_stage]['microservices'])
                    if (ms_len-1)==current_index:
                        #存入数据库,只有每个stage的最后一个topic才存入数据库
                        configs=mes['output']['stages'][current_stage]['store']
                        to_db(res,configs)
                        #寻找next列表
                        next=mes['output']['stages'][current_stage]['next']
                        if len(next)==0:
                            to_control({
                                "type":"finished",
                                "workerid":server_id,
                                "taskid":mes['taskid'],
                                "child_id":mes['childid'],
                                "task_finished":True,
                                "finish_info":"next list is null",
                                "error":False,
                                "err_msg":""
                            },"next list is null")
                        else:
                            depth=mes['ouput']['depth']+1
                            #在next列表中获取stage并取topic发送消息
                            for item in next:
                                topic=mes['output']['stages'][item]['microservices'][0]['topic']
                                new_mes=mes
                                new_mes['data']=res
                                new_mes['output']['current_stage']=item
                                new_mes['output']['current_index']=0
                                new_mes['output']['depth']=depth
                                to_next(new_mes.topic)
                            to_control({
                                "type":"running",
                                "workerid":server_id,
                                "taskid":mes['taskid'],
                                "child_id":mes['childid'],
                                "task_finished":False,
                                "info":"send mes to next topics",
                                "error":False,
                                "err_msg":""
                            },"send mes to next topics")
                            return
                    else:
                        #当前stage寻找topic
                        topic=mes['output']['stages'][current_stage]['microservices'][current_index+1]['topic']
                        new_mes=mes
                        new_mes['data']=res
                        new_mes['output']['current_index']=current_index+1
                        #将消息发送给下一个topic
                        to_next(new_mes.topic)
                        #消息反馈
                        to_control({
                            "type":"running",
                            "workerid":server_id,
                            "taskid":mes['taskid'],
                            "child_id":mes['childid'],
                            "task_finished":False,
                            "info":"send mes to next topic",
                            "error":False,
                            "err_msg":""
                        },"send mes to next topic")
                        return
            else:
                to_control({
                    "state":"task finish",
                    "info":"data has been deat"
                },"task has been deat")
                return

        except Exception as err:
            traceback.print_exc()
            
#控制消息和执行消息的分类处理
def mes_classify(mes,flag):
    if flag==0:
        temp=set(mes['taskid'])
        if mes['type']=="stop":
            stop_task=stop_task|temp
            to_control({
                "info":"tasks stop success!"
            },"tasks stop success")
            return
        else:
            stop_task=stop_task-(stop_task&temp)
            to_control({
                "info":"tasks restart success!"
            },"tasks restart success")
            return
    else:
        if mes['taskid'] in stop_task:
            to_control({
                "info":"task has been stop"
            },"task has been stop")
            return
        else:
            mes_analysis(mes)
            return


#kafka生产者
def sendmes(mes,topic):
    producer=kafka.KafkaProducer(bootstrap_servers = kafka_cluster)
    mesg=str(json.dumps(mes)).encode('utf-8')
    try:
        producer.send(topic, mesg)
        print("send data successfully!")
    except Exception as err:
        producer.close()
        print(str(err))
    producer.close()


app=Sanic()


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
                    
                    global server_id
                    global server_lowertopic
                    global server_hightopic
                    global server_controltopic
                    global server_state

                    server_id=temp['id']
                    server_lowertopic=temp['topic']['low_priority']
                    server_hightopic=temp['topic']['high_priority']
                    server_controltopic=temp['topic']['controller']
                    server_state=temp['state']

                    if server_state:
                        logger.info('register service success!  '+'server_id:'+server_id)
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
    
    mes_always_get()

#从指定的topic和group_id中获取一条消息
def get_one_mes(topic,group_id):
    try:
        consumer = kafka.KafkaConsumer(group_id=group_id,bootstrap_servers=kafka_cluster)
        consumer.subscribe(topics=(topic))
        mess=consumer.poll(timeout_ms=5,max_records=1)
        if len(mess)!= 0:
            for key in mess.keys():
                mes=json.loads(mess[key][0].value.decode('utf-8'))
            consumer.close()
            logger.info("gain one message in "+str(group_id))
            return mes
        else:
            logger.info("no message get in "+str(group_id))
            consumer.close()
            return None
    except Exception as err:
        consumer.close()
        logger.error("Error meet during get message in "+str(group_id))
        traceback.print_exc()
        return None

#消息获取循环
def mes_always_get():
    mes_sign=[]
    mes_sign.append({
        "topic":server_controltopic,
        "flag":0,
        "group_id":control_group_id
    })
    mes_sign.append({
        "topic":server_hightopic,
        "flag":1,
        "group_id":h_group_id
    })
    mes_sign.append({
        "topic":server_lowertopic,
        "flag":1,
        "group_id":l_group_id
    })

    while True:
        for i in range(0,3):
            mes=get_one_mes(mes_sign[i]['topic'],mes_sign[i]['group_id'])
            if mes!=None:
                print(mes)
                mes_classify(mes,mes_sign[i]['flag'])
                break
            else:
                continue
        time.sleep(0.5)
    # print(consumer.topics())
    #获取当前client订阅的分区
    # temp=consumer.assignment()
    # print(temp)
    # print(str(temp))
    # for item in consumer:
    #     print(str(consumer))

    # for item in temp:
    #     if item[0]==server_controltopic:
    #         begin_offset=consumer.beginning_offsets([item])[item]
    #         end_offset=consumer.end_offsets([item])[item]
    #         print("begin:"+begin_offset+"end:"+end_offset)
    #         if end_offset == (begin_offset+1):
    #             break
    #         else:
    #             consumer.seek_to_beginning([item])
    #             for mess in consumer:
    #                 logger.info("get one message from controller")
    #                 mes['type']="control"
    #                 mes['info']=dict(mess.value.decode('utf-8'))
    #                 mes=json.loads(mes)
    #                 break
    #             break
                
@app.get(heathcheck_path)
async def health_check(request):

    #微服务执行函数的健康检查

    return sjson({
        "state":0,
        "infor":"health"
    })


if __name__ == "__main__":
    # mes_always_get()
    if len(server_ip) != 0 and len(server_port) !=0:
        app.run(server_ip,server_port)
    else:
        logger.info('start server fail,lack ip or port')
