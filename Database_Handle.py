import pymongo
from retrying import retry
import datetime
# import pymysql
# from Micro_Logger import deal_log
# from mysql_fun import insert


class MongoDB_Store():

    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
    def store_data_list(self, url, db_name, col_name, data_list, result_list):
        print(datetime.datetime.now())
        try:
            client = pymongo.MongoClient(url)
            db = client[db_name]
            col = db[col_name]
            for i in range(len(data_list)):
                col.insert(
                    {
                        "key": data_list[i],
                        "value": result_list[i]
                    }
                )
        except Exception as e:
            raise


class MySql_Store(object):

    def insert(self, sql):
        print("1")

if __name__ == "__main__":
    url = "mongodb://root:123456@127.0.0.1:27017"
    db_name = "iii"
    col_name = "iii"
    data_list = [1,2,2,4]
    result_list = [1,2,2,4]
    handle = MongoDB_Store()
    handle.store_data_list(url, db_name, col_name, data_list, result_list)