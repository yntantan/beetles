import socketserver
from xmlrpc.server import SimpleXMLRPCServer
import helper
import settings
from multiprocessing import Queue, Process
import logging 

logger = logging.getLogger(__name__)

tasks = ['http://tech.163.com', 'http://ent.163.com', 'http://news.163.com', 'http://auto.163.com',
             'http://war.163.com', 'http://money.163.com', 'http://fashion.163.com', 'http://jiankang.163.com']

#tasks = ['http://www.10010.com']

class DispatcherRPCServer(socketserver.ThreadingMixIn, SimpleXMLRPCServer):
    pass
    
class ManageDownloader:
    def __init__(self, recv_q, newtasks_q):
        self.downloaders = dict()
        self.new_q = newtasks_q
        self.recv_q = recv_q
        self.failed_tasks = dict()
        
    def register_downloader(self, name, manager_addr):
        if name in self.downloaders.keys():
            return settings.EXIST_DOWNLOADER
        else:
            tmp = dict()
            tmp[settings.MANAGER_ADDR] = manager_addr           
            self.downloaders[name] = tmp
            return settings.SUCCESS_REGISTER
    
    def get_tasks(self, name, num):
        num, ret = self._get_failed_tasks(name, num)     
        while num > 0 and not self.new_q.empty():
            ret.append(self.new_q.get())
            num -= 1
        return ret 
        
    def send_results(self, results):
        logger.info('received {} good results'.format(len(results)))
        for result in results:
            recv_q.put_nowait(result)
        
        
    def send_failed_results(self, name, results):
        logger.info('received {} failed results'.format(len(results)))
        for result in results:
            if result['url'] in self.failed_tasks:
                self.failed_tasks[result['url']].add(name)
            else:
                tmp = set()
                tmp.add(result['url'])
                self.failed_tasks[result['url']] = tmp
                    
    def _get_failed_tasks(self, name, num):
        ret = []
        for key, val in self.failed_tasks.items():
            if num == 0:
                break
            if name in val:
                continue
            ret.append(key)
            num -= 1
        return num, ret


                
def deduper(recv_q, new_q):
    
    logger.info('in deduper process!')
    def store(result):
        pass
        
    fix_url = lambda addr: addr if ':' in addr \
                    else 'http://{}'.format(addr)
                        
    seen_urls = set(tasks)
    for url in seen_urls:
        new_q.put_nowait(url)
    loggertimes = 10
    iternum = 0
    try:  
        f = open('link.txt', 'w')  
        while True:
            iternum += 1
            result = recv_q.get()
            result['new_urls'] = set(result['new_urls']) if result['new_urls'] is not None else set()
            for url in set(result['new_urls']).difference(seen_urls):
                f.writelines(url)
                f.writelines('\n')
                store(result)
                if result['next_url'] is None:
                    new_q.put_nowait(url)
            seen_urls.update(set(result['new_urls']))
            if iternum % loggertimes == 0:
                logger.info('---------------------------------{}----------------------------'.format(len(seen_urls)))
    finally:
        f.close()
    
           
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.FORMAT)
    with DispatcherRPCServer(('localhost', 8500), allow_none=True) as server:
        new_q = Queue()
        recv_q = Queue()
        server.register_instance(ManageDownloader(recv_q, new_q))
        Process(target=deduper, args=(recv_q, new_q)).start()
        print ('serve on localhost:8500')
        server.serve_forever()
    
    