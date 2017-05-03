import threading
import time
import logging
import settings
logger = logging.getLogger(__name__)

class Node:
    def __init__(self, time=None, tasks=None,
                       flag=None, pre=None, _next=None):
        self.time = time
        self.tasks = tasks
        self.flag = flag
        self.pre = pre
        self.next = _next
        
class BList:
    
    def __init__(self):
        self.head = Node()
        self.tail = self.head
        self.size = 0
    
    def add(self, newnode):
        self.tail.next = newnode
        newnode.pre = self.tail
        self.tail = newnode
        self.size += 1
            
    def remove(self, node):
        if node is self.tail:
            self.tail = node.pre
            node.pre = None
            self.tail.next = None
        else:
            node.pre.next = node.next
            node.next.pre = node.pre
        self.size -= 1
    
    def pop(self):
        if self.head.next is None:
            return None
        ret = self.head.next
        self.remove(self.head.next)
        return ret
        
    def peak(self):
        return self.head.next
                    
    def __str__(self):
        step = self.head.next
        slist = []
        while(step is not None):
            slist.append(str(step.tasks))
            step = step.next
        return '---'.join(slist)
        
    def __len__(self):
        return self.size

class Timer:
    '''
    data: addr:port time tmpTasks
    node: time
    map: key:value -> "addr:port":[tasks]
    what i need to do is:
    1. give a ip:pid, store the key and tasks
    2. add node with current_time to list 
    '''
    def __init__(self, q, timeout=settings.TIME_OUT):
        self._blist = BList()
        self._map = dict()
        self.queue = q
        self.thread = None
        self.timeout = timeout
        self.lock = threading.Lock()
    
    def _fix(self):
        #self.lock.acquire()
        node = self._blist.pop()
        for task in node.tasks:
            self.queue.put_nowait(task)
        #del self._map[node.flag]
        if len(self._blist) > 0:
            interval = self._blist.peak().time - node.time
            self.thread = threading.Timer(interval, self._fix)
            self.thread.start()
        else:
            self.thread = None
        #self.lock.release()
        logger.info('redistribute: {}, list\'s length: {}'.format(node.flag, len(self._blist)))
            
    def add(self, flag, tasks):
        #self.lock.acquire()
        node = Node(time.time(), tasks, flag)
        self._blist.add(node)
        self._map[flag] = node
        if self.thread is None:
            self.thread = threading.Timer(self.timeout, self._fix)
            self.thread.start()
        #self.lock.release()
        #logger.info('add: {}, list\'s length: {}'.format(flag, len(self._blist)))
        #logger.info(self._blist)
    
    
    def remove(self, flag):
        #self.lock.acquire()
        if self._blist.peak() is self._map[flag]:
            node = self._blist.pop()            
            self.thread.cancel()
            if len(self._blist) > 0:
                interval = self._blist.peak().time + self.timeout - time.time()
                self.thread = threading.Timer(interval, self._fix)
                self.thread.start()
            else:
                self.thread = None
        else:
            self._blist.remove(self._map[flag])
        #self.lock.release()
        #logger.info('remove: {}, list\'s length: {}'.format(flag, len(self._blist)))
        #logger.info(self._blist)
    
    
    
if __name__=='__main__':
    node = Node(1)
    node2 = Node(2)
    node3 = Node(3)
    bl = BList()
    bl.add(node)
    print(len(bl))
    bl.add(node2)
    print(bl)
    bl.add(node3)
    print(bl)
    bl.remove(node)
    print(bl)
    bl.add(node)
    print(bl)
    a = dict()
    a[node]= 'hello'
    print(a)
    
    
    
