import mysql.connector as sqlconn
import logging
import settings
import sys
import socket
from collections import namedtuple

logger = logging.getLogger(__name__)


FetchStatistic = namedtuple('FetchStatistic',
                            ['url',
                             'status',
                             'exception',
                             'size',
                             'content_type',
                             'body'])
                             
                             
def connect():
    try:
        db_conn = sqlconn.connect(host = settings.db_host,
                                  port = settings.db_port,
                                  user = settings.db_user,
                                  password = settings.db_pass,
                                  database = settings.db_name)
    except:
        logger.error(sys.exc_info())
        quit()
    return db_conn
        
        
def get_tasks():
    tasks = processSQL('select id, url from {} where is_new=%s'.format(settings.task_table), (True,))
    return tasks
        
def get_crawlers():
    crawlers = processSQL('select id, addr_ip, addr_port from {} \
                            where running=%s order by id, count'.format(settings.crawler_table), (True,))
    return crawlers
 
def process_sql(sql, arg=None):
    db_conn = connect()
    cursor = db_conn.cursor()
    cursor.execute(sql, arg)
    data = cursor.fetchall()
    cursor.close()
    db_conn.close()
    return data
    

def communicate(host, port, request):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    s.send(request.encode("utf-8"))
    response = s.recv(1024).decode("utf-8")
    s.close()
    return response
    

                       
if __name__ == "__main__":
    logging.basicConfig(format=settings.FORMAT, level=logging.INFO)
    logger.info('test utils.py')
    db_conn = connect()
    cursor = db_conn.cursor()
    logger.info('excute \'show tables;\'')
    cursor.execute('show tables')
    values = cursor.fetchall()
    logger.info('results: {}'.format(values))
    logger.info('getTasks')
    if len(getTasks()) == 0:
        logger.info('getTasks is None')
    logger.info('getCrawlers')
    getCrawlers()
        