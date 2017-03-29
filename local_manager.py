import socketserver
from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
import crawler
import settings
import time
import multiprocessing
import asyncio
import logging
import argparse
import hashlib
import os

logger = logging.getLogger(__name__)

class LocalManagerRPCServer(socketserver.ThreadingMixIn, SimpleXMLRPCServer):
    processes = dict()
    

def construct_name(index, pid, addr):
    m = hashlib.sha256()
    m.update(str(index).encode('utf-8'))
    m.update(str(pid).encode('utf-8'))
    m.update(str(addr).encode('utf-8'))
    return m.hexdigest()
    
def download(master_addr, name, manager_addr):
    host, port = master_addr
    logger.info('download name: {}'.format(name))
    proxy = xmlrpc.client.ServerProxy('http://{}:{}/'.format(host, port), allow_none=True) 
    while True:          
        try:
            retcode = proxy.register_downloader(name, manager_addr)
            break
        except xmlrpc.client.ProtocolError as err:
            time.sleep(settings.CONNECTIONREFUSED_SLEEP)
        
    if retcode == settings.EXIST_DOWNLOADER:
        logger.warn('this downloader has been registed') 
    loop = asyncio.get_event_loop()  
    try:
        while True:            
            tasks = proxy.get_tasks(name, settings.MAX_CRAWLER_NUM)
            if len(tasks) == 0:
                logger.info('no more tasks!')
                time.sleep(settings.NO_TASKS_SLEEP)
                continue
            cr = crawler.Crawler(tasks)
            loop.run_until_complete(cr.crawl())
            proxy.send_failed_results(name, cr.fail_done)
            proxy.send_results(cr.done)
            cr.close()
    finally:
        cr.close()
        loop.stop()
        loop.run_forever()
        loop.close()
                
                
            
              
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.FORMAT)
    parser = argparse.ArgumentParser()
    parser.add_argument('--port','-p',
                        help='manager\'s port',
                        action='store')
    parser.add_argument('--master','-m',
                        help='master\'s address host:port',
                        action='store')
    parser.add_argument('--processnum', '-n',
                        help='process number to crawl',
                        default='1',
                        action='store')
                                            
    args = parser.parse_args()
    localhost = 'localhost'
    localport = int(args.port)
    master_host, master_port = args.master.split(':')
    master_port = int(master_port)
    num = int(args.processnum)
    
    with LocalManagerRPCServer((localhost, localport)) as manager:
        for i in range(num):
            name = construct_name(i, os.getpid(), (localhost, localport))
            manager.processes['name']= multiprocessing \
                            .Process(target = download, 
                             args = ((master_host, master_port), name, 
                             (localhost, localport)))
        for p in manager.processes.values():
            p.start()
        manager.serve_forever()
    