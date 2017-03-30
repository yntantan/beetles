import threading
import time
import logging

class Node:
    def __init__(self, val, pre=None, _next=None):
        self.val = val
        self.pre = pre
        self.next = _next
        
class BList:
    
    def __init__(self):
        self.head = None
        self.tail = None
        self.size = 0
    
    def add(self, newnode):
        if self.head is None:
            self.head = newnode
            self.tail = newnode
        else:
            newnode.pre = self.tail
            newnode.next = None
            self.tail.next = newnode
            self.tail = newnode  
            
    def remove(self, node):
        if node is self.head:
            self.head = node.next
            self.head.pre = None
            node.next = None
        elif node is self.tail:
            self.tail = node.pre
            self.tail.next = None
            node.pre = None
        else:
            node.pre.next = node.next
            node.next.pre = node.pre
            node.pre = None
            node.next = None
            
    def __str__(self):
        step = self.head
        slist = []
        while(step is not None):
            slist.append(str(step.val))
            step = step.next
        return '-'.join(slist)

class Timer:
    '''
    data: addr:port time tmpTasks
    node: time
    map: key:value -> "addr:port":[tasks]
    what i need to do is:
    1. give a addr:port, store the key and tasks
    2. add node with current_time to list 
    '''
    def __init__(self, q):
        self.blist = BList()
        self.map = dict()
        self.queue = q
        
    def 
           
        
def hello():
    print('hello {}'.format(threading.current_thread().name))
    
    
    
if __name__=='__main__':
    node = Node(1)
    node2 = Node(2)
    node3 = Node(3)
    bl = BList()
    bl.add(node)
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
    
    
    
