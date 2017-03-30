#for logging
FORMAT = '%(asctime)-15s %(name)s module:%(module)s, %(message)s'

#dispatcher
EMPTY_TASK_TIME = 10
PERIODIC_DUTY_INTERVAL = 10


#spider_runner   
#check interval
CHECK_INTERVAL = 5
#excute interval
EXECUTE_INTERVAL = 0
#
MAX_CRAWLER_NUM = 1
#
NO_TASKS_SLEEP = 5

CONNECTIONREFUSED_SLEEP = 1

EXIST_DOWNLOADER = 1
SUCCESS_REGISTER = 2

MANAGER_ADDR = 'manager_addr'

FAILED_TASKS = 'failed_tasks'

#for database
db_host = 'localhost'
db_port = 3306
db_user = 'crawler'
db_pass = 'asdfnihao'
db_name = 'spiderdb'

task_table = 'task_table'
crawler_table = 'crawler_table'