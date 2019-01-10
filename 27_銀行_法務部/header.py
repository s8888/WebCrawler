import logging

logging.basicConfig(level=logging.INFO, #DEBUD, INFO, WARNING, ERROR, CRITICAL
#                     filename='./log.txt',
                    filemode='w',
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
