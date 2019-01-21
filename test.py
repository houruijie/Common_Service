#测试程序的执行
from service import Service

sv=Service("./init.ini")


def before_request(single_input_item, config):
    print(single_input_item)
    return single_input_item

sv.register_before_request(before_request)



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







