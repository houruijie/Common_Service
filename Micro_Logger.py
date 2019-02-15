import logging

# Step 1: Loggers, 并设置全局level
logger = logging.getLogger('logging_blog')
logger.setLevel(logging.DEBUG)
# Step 2: Handler
# print to screen
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# # write to file
# fh = logging.FileHandler("log_file.log")
# fh.setLevel(logging.DEBUG)
# fh.close()


# 函数功能：打印log
def deal_log(message, level="info", file_path="log_file.log"):
    # write to file
    fh = logging.FileHandler(file_path)
    fh.setLevel(logging.DEBUG)
    # Step 3: Formatter
    my_formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s]%(message)s')
    ch.setFormatter(my_formatter)
    fh.setFormatter(my_formatter)
    logger.addHandler(ch)
    logger.addHandler(fh)
    # debug、info、warning、error以及critical
    # level = level.lower()
    if level == "debug":
        logger.debug(message)
    elif level == "info":
        logger.info(message)
    elif level == "warning":
        logger.warning(message)
    elif level == "error":
        logger.error(message)
    elif level == "critical":
        logger.critical(message)
    else:
        logger.error("Error: the logger level is wrong")
    logger.removeHandler(fh)


if __name__ == "__main__":

    deal_log("debug test", "debug")
    deal_log("info test", "info")
    deal_log("error test", "error")
    deal_log("error test3", "error")
