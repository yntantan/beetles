import socketserver
from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import helper
import settings
from multiprocessing import Queue, Process
import logging 
import threading 
from timer import Timer

logger = logging.getLogger(__name__)

localrecord = threading.local()

tasks = ['http://tech.163.com', 'http://ent.163.com', 'http://news.163.com', 'http://auto.163.com',
             'http://war.163.com', 'http://money.163.com', 'http://fashion.163.com', 'http://jiankang.163.com']

#tasks = ['http://www.xidian.edu.cn']

class RequestHandler(SimpleXMLRPCRequestHandler):
    def __init__(self, request, client_address, server):
        localrecord.ip, _ = client_address
        SimpleXMLRPCRequestHandler.__init__(self, request, client_address, server)
        
        
        
class DispatcherRPCServer(socketserver.ThreadingMixIn, SimpleXMLRPCServer):
    connected_client = set()
    
class ManageDownloader:
    def __init__(self, recv_q, newtasks_q):
        self.downloaders = dict()
        self.new_q = newtasks_q
        self.recv_q = recv_q
        self.failed_tasks = dict()
        self.timer = Timer(self.new_q)
        
    def _get_flag(self, pid):
        return "{}:{}".format(localrecord.ip, pid)
            
    def get_tasks(self, pid, num):
        flag = self._get_flag(pid)
        logger.info('{} request new tasks'.format(flag))
        num, ret = self._get_failed_tasks(flag, num)     
        while num > 0 and not self.new_q.empty():
            ret.append(self.new_q.get())
            num -= 1
        #this is for timer
        #1.record flag and tasks(stored in ret)
        #2.start Timer
        #self.timer.add(flag, tasks)
        self.timer.add(flag, ret)
        return ret 
        
    def send_results(self, pid, results):
        flag = self._get_flag(pid)
        logger.info('received {} good results from {}'.format(len(results), flag))
        self.timer.remove(flag)
        for result in results:
            recv_q.put_nowait(result)
        
        
    def send_failed_results(self, pid, results):
        flag = self._get_flag(pid)
        logger.info('received {} failed results'.format(len(results)))
        for result in results:
            if result['url'] in self.failed_tasks:
                self.failed_tasks[result['url']].add(flag)
            else:
                tmp = set()
                tmp.add(flag)
                self.failed_tasks[result['url']] = tmp
                    
    def _get_failed_tasks(self, flag, num):
        ret = []
        for key, val in self.failed_tasks.items():
            if num == 0:
                break
            if flag in val:
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
    with DispatcherRPCServer(('localhost', 8500), requestHandler=RequestHandler,
                                                  logRequests=False,
                                                  allow_none=True) as server:
        new_q = Queue()
        recv_q = Queue()
        server.register_instance(ManageDownloader(recv_q, new_q))
        Process(target=deduper, args=(recv_q, new_q)).start()
        print ('serve on localhost:8500')
        server.serve_forever()
    
    