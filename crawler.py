import asyncio
import logging
import urllib.parse
import time
import cgi
import re
import urllib.parse

try:
    from asyncio import JoinableQueue as Queue
except ImportError:
    from asyncio import Queue

import aiohttp
import settings
import threading


logger = logging.getLogger(__name__)

def fetchstatistic(url, status, exception, size, content_type, encoding, body, new_urls):
    ret = dict()
    ret['url'] = url
    ret['status'] = status
    ret['exception'] = exception
    ret['size'] = size
    ret['content_type'] = content_type
    ret['encoding'] = encoding
    ret['body'] = body
    ret['new_urls'] = list(new_urls)
    return ret
    
    
class Crawler:
    
    def __init__(self, roots, max_tries=1, 
                    max_tasks=1, q=None,
                    loop=None):
        self.loop = loop or asyncio.get_event_loop()
        logger.info('init crawler...')
        self.roots = roots
        self.max_tasks = max_tasks
        self.max_tries = max_tries
        self.q = q or Queue(loop=self.loop)
        self.seen_urls = set()
        self.done = []
        self.fail_done = []
        for root in roots:
            self.q.put_nowait(root)
            self.seen_urls.add(root)
        self.session = aiohttp.ClientSession(loop=self.loop)
        logger.info('init done! queue size:{}'.format(self.q.qsize()))
    
    def clear(self):
        self.done.clear()
        self.fail_done.clear()
            
    def close(self):
        self.session.close()
    
    def record_statistic(self, fetch_statistic):
        self.done.append(fetch_statistic)
    
    def record_failed_statistic(self, fetch_statistic):
        self.done.append(fetch_statistic)
    
    def add_roots(self, roots):
        for root in roots:
            self.q.put_nowait(root)  
                      
    async def parse_links(self, response):
        content_type = None
        encoding = None
        body = await response.read()
        new_urls = None
        if response.status == 200:
            content_type = response.headers.get('content-type')
            pdict = {}

            if content_type:
                content_type, pdict = cgi.parse_header(content_type)

            encoding = pdict.get('charset', 'utf-8')
            if content_type in ('text/html', 'application/xml'):
                text = await response.text()
                
                urls = set(re.findall(r'''(?i)href=["']([^\s"'<>]+)''',
                                      text))
                if urls:
                    logger.info('got %r distinct urls from %r',
                                len(urls), response.url)
                new_urls=set()                
                for url in urls:
                    normalized = urllib.parse.urljoin(response.url, url)
                    defragmented, frag = urllib.parse.urldefrag(normalized)
                    new_urls.add(defragmented)
                    self.seen_urls.update(new_urls)
        stat = fetchstatistic(
            url=response.url,
            status=response.status,
            exception=None,
            size=len(body),
            content_type=content_type,
            encoding=encoding,
            body=body,
            new_urls=new_urls)   
        return stat  
        
    async def fetch(self, url):
        tries = 0
        exception = None
        while tries < self.max_tries:
            try:
                response = await self.session.get(
                    url, allow_redirects=False)
                    
                if tries > 1:
                    logger.info('try {} for {} success'.format(tries, url))
                break
            except aiohttp.ClientError as client_error:
                logger.info('try {} for {} raised {}'.format(tries, url, client_error))
                exception = client_error
            tries += 1
        else:
            logger.error('{} failed after {} tries'.format(
                         url, self.max_tries))
            self.record_failed_statistic(fetchstatistic(url=url,
                                                  status=None,
                                                  exception=exception,
                                                  size=0,
                                                  content_type=None,
                                                  encoding=None,
                                                  body=None,
                                                  new_urls=None))
            return
        logger.info('parse link {}'.format(response.url))
        stat = await self.parse_links(response)
        logger.info('url: {} status: {} size: {}'.format(stat['url'], stat['status'], stat['size']))
        self.record_statistic(stat)
        await response.release()
        
    async def work(self):
        try:
            while True:
                url = await self.q.get()
                await self.fetch(url)
                self.q.task_done()
        except asyncio.CancelledError:
            logger.info('received cancel signal!!')
            

    async def crawl(self):
        logger.info('init crawl, construct workers')
        workers = [self.loop.create_task(self.work())
                    for _ in range(self.max_tasks)]
        self.t0 = time.time()
        logger.info('time: {}'.format(self.t0))
        await self.q.join()
        self.t1 = time.time()
        for w in workers:
            w.cancel()

def test(roots, q):
    for root in roots:
        logger.info('put url:{} to queue'.format(root))
        q.put_nowait(root)
        
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.FORMAT)
    roots1 = ['http://tech.163.com', 'http://ent.163.com', 'http://news.163.com', 'http://auto.163.com']
    roots2 = ['http://war.163.com', 'http://money.163.com', 'http://fashion.163.com', 'http://jiankang.163.com']
    loop = asyncio.get_event_loop()
    q = Queue()
    crawler = Crawler(roots1, loop=loop, q=q)
    #threading.Thread(target=test, args=(roots2, q)).start()
    try:
        loop.run_until_complete(crawler.crawl())  # Crawler gonna crawl.
    except KeyboardInterrupt:
        sys.stderr.flush()
        print('\nInterrupted\n')
    finally:
        crawler.close()
        loop.stop()
        loop.run_forever()
        loop.close()        