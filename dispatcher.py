import socketserver
from xmlrpc.server import SimpleXMLRPCServer
import helper
import settings

tasks = ['http://tech.163.com', 'http://ent.163.com', 'http://news.163.com', 'http://auto.163.com',
             'http://war.163.com', 'http://money.163.com', 'http://fashion.163.com', 'http://jiankang.163.com']

class DispatcherRPCServer(socketserver.ThreadingMixIn, SimpleXMLRPCServer):
    pass
    
class ManageDownloader:
    def __init__(self, tasks, server):
        self.server = server
        self.downloaders = dict()
        self.seen_tasks = set(tasks)
        self.rest_tasks = self.seen_tasks.copy()
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
        while num > 0 and len(self.rest_tasks) > 0:
            ret.append(self.rest_tasks.pop())
            num -= 1
        return ret 
        
    def send_results(results):
        print('get results')
        return True
        
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
                
        
if __name__ == '__main__':
    with DispatcherRPCServer(('localhost', 8000)) as server:
        server.register_instance(ManageDownloader(tasks, server))
        print ('serve on localhost:8000')
        server.serve_forever()
    
    