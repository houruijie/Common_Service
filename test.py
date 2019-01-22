# 测试程序的执行
from service import Service
from sanic.response import json

sv = Service("./init.ini")


# @sv.predeal_data()
# def predeal(data,config):
#     return data

@sv.before_request()
def before_request(single_input_item):
    # print(single_input_item)
    single_request_item=single_input_item
    return single_request_item


#斐波那契处理函数接受 data_list中单个进行预处理后的输出作为输入,config
@sv.handle_input_item()
def handle_input_item(single_request_item):
    if single_request_item ==1:
        return 1
    elif single_request_item==2:
        return 1
    else:
        a=1
        b=1
        while single_request_item > 2:
            c=a
            a=b
            b=c+b
            single_request_item=single_request_item-1
        return b


#将单个处理函数的输出作为输入
@sv.after_request()
def after_request(single_response_item):
    # print(single_response_item)
    return single_response_item


#

#


@sv.health_check()
def health_check(request):
    return json({
        "status": "health",
        "infor": "wwwww"
    })


# data=["121212"]
# config=None
# predeal(data,config)
sv.run()


# @service.after_received()
# def after_received(input_item_list)

#     return input_item_list


# # 每个请求发出之前, 封装单个 API 请求 的 Request的钩子函数
# @service.before_request()
# def before_request(single_input_item, config)

#     # 处理单个消息的输入列表的单个输入的处理逻辑
#     header, params, body = handle(single_input_item)

#     single_item_request = {
#         header: header,
#         params: params,
#         body: body
#     }

#     return single_item_request

# # 每个请求发出之前, 处理单个 API 请求的 response的钩子函数
# @service.after_request()
# def after_request(response)

#     # 处理单个消息的输入列表的处理逻辑
#     output_item = extract(response)

#     return output_item

# @service.before_finished()
# def before_finished(output_item_list)

#     return output_item_list


# service = Service("Cert Collector")
# service.init(external_api="http://ip:port/path", config="path_to_cofig", **kwargs)
# servcie.start()


# # 1
# @service.handle_input_item(strategy="evenlet | thread | process")
# def handle_input_item(single_input, config)

#     # 处理单个消息的输入列表的单个输入的处理逻辑
#     output_item = handler(single_input)

#     return output_item

# # 2
# @service.handle_input_item(strategy="evenlet | thread | process")
# def handle_input_item(input_item_list, config)

#     # 处理单个消息的输入列表的处理逻辑
#     output_item_list = multiProcessHandler(input_item_list)

#     return output_item_list


# service = Service("Cert Collector")
# service.init(config="path_to_cofig", **kwargs)
# servcie.start()
