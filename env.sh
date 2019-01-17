#!/bin/sh
#API相关
#请求的url地址
# export request_url="http://10.245.146.42:7777/hello"
# #请求的方式
# export request_type="post"
# #请求所需要的参数
# export paras=['domain','type','mode']
# #参数携带的方式  1.URL中携带 2.请求体中携带 3.其他方式携带
# export para_usetype=1

#接受kafka消息相关
export kafka_cluster=['127.0.0.1:9092']
# export receive_topic="testtopic"
# export consume_id="consume1"

# #服务注册和健康度检查相关
export server_ip="10.246.148.166"
export server_port="3000"
export server_name="testservice"
export server_type="DNS"


# # export server_meta=["redis_version": "4.0"]
# export heathcheck_script_dir="/usr/local/bin/check_redis.py"
# export heathcheck_path="/health"


#服务注册和健康度检查相关
# export server_ip=""
# export server_port=""
# export server_name=""
# export server_type=""

# export server_meta=["redis_version": "4.0"]
export heathcheck_args=""
export heathcheck_path=""




