import asyncio
import cgi
import logging
import re
import time
import urllib.parse
from xmlrpc.client import ServerProxy
import os
import settings
import argparse
import pickle
import aiohttp
import hashlib
from bs4 import BeautifulSoup

try:
    # Python 3.4.
    from asyncio import JoinableQueue as Queue 
except ImportError:
    # Python 3.5.
    from asyncio import Queue

logger = logging.getLogger(__name__)


def lenient_host(host):
    parts = host.split('.')[-2:]
    return ''.join(parts)


def is_redirect(response):
    return response.status in (300, 301, 302, 303, 307)


def fetchstatistic(url, next_url, status, 
                   exception, size, content_type,
                   encoding, new_urls, body):
    ret = dict()
    ret['url'] = url
    ret['next_url'] = next_url
    ret['status'] = status
    ret['exception'] = exception
    ret['size'] = size
    ret['content_type'] = content_type
    ret['encoding'] = encoding
    ret['new_urls'] = list(new_urls)
    if body is not None:
        ret['body'] = pickle.dumps(body)
    else:
        ret['body'] = None
    return ret


#
class Crawler:
    def __init__(self, roots,
                 exclude=None,  session=None,# What to crawl.
                 max_redirect=10, max_tries=1,  # Per-url limits.
                 max_tasks=50, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.roots = roots
        self.exclude = exclude
        self.max_redirect = max_redirect
        self.max_tries = max_tries
        self.max_tasks = max_tasks
        self.q = Queue(loop=self.loop)
        self.seen_urls = set()
        self.done = []
        self.fail_done=[]
        self.session = session or aiohttp.ClientSession(loop=self.loop)
        self.root_domains = set()
        for root in roots:
            parts = urllib.parse.urlparse(root)
            host, port = urllib.parse.splitport(parts.netloc)
            if not host:
                continue
            #this is for ip addresses
            if re.match(r'\A[\d\.]*\Z', host):
                self.root_domains.add(host)
            else:
                host = host.lower()
                self.root_domains.add(host)
        for root in roots:
            self.add_url(root)
        self.t0 = time.time()
        self.t1 = None

    def close(self):
        """Close resources."""
        self.session.close()
    
    def host_okay(self, host):
        host = host.lower()
        if host in self.root_domains:
            return True
        if re.match(r'\A[\d\.]*\Z', host):
            return False
        return self._host_okay_strictish(host)
        
    def _host_okay_strictish(self, host):
        host = host[4:] if host.startswith('www.') else 'www.' + host
        return host in self.root_domains
    
    def _parse_allow(self, url):
        typ = ('png', 'gif', 'jpg', 'css', 'js')
        for t in typ:
            if url.endswith(t):
                return False
        return True       
            
    def record_statistic(self, fetch_statistic):
        self.done.append(fetch_statistic)
    
    def record_failed_statistic(self, fetch_statistic):
        self.fail_done.append(fetch_statistic)
        
    @asyncio.coroutine
    def parse_links(self, response):
        links = set()
        content_type = None
        encoding = None
        body = yield from response.read()
        text = None
        if response.status == 200:
            content_type = response.headers.get('content-type')
            pdict = {}

            if content_type:
                content_type, pdict = cgi.parse_header(content_type)

            encoding = pdict.get('charset', 'utf-8')
            if content_type in ('text/html', 'application/xml'):
                text = yield from response.text()
                sp = BeautifulSoup(text)
                # Replace href with (?:href|src) to follow image links.
                # urls = set(re.findall(r'''(?i)href=["']([^\s"'<>]+)''',
                #                       text))
                # urls = set(re.findall(r'''http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+''',
                #                       text))
                urls = []
                for link in sp.find_all('a'):
                    try:
                        urls.append(link.get('href'))
                    except Exception:
                        continue
                for url in urls:
                    normalized = urllib.parse.urljoin(response.url, url)
                    defragmented, frag = urllib.parse.urldefrag(normalized)
                    if self.url_allowed(defragmented):
                        links.add(defragmented)
                tmpset = sp.find_all(id='wz_zw')
                if len(tmpset) > 0:
                    logger.info('news: {}'.format(response.url))
                    text = tmpset[0].get_text()

        stat = fetchstatistic(
            url=response.url,
            next_url=None,
            status=response.status,
            exception=None,
            size=len(body),
            content_type=content_type,
            encoding=encoding,
            new_urls=links,
            body=text)

        return stat, links


    @asyncio.coroutine 
    def fetch(self, url, max_redirect):
        """Fetch one URL."""
        tries = 0
        exception = None
        while tries < self.max_tries:
            try:
                response = yield from self.session.get(
                    url, allow_redirects=False)

                if tries > 1:
                    logger.info('try %r for %r success', tries, url)

                break
            except aiohttp.ClientError as client_error:
                logger.info('try %r for %r raised %r', tries, url, client_error)
                exception = client_error

            tries += 1
        else:
            # We never broke out of the loop: all tries failed.
            logger.error('%r failed after %r tries',
                         url, self.max_tries)
            self.record_failed_statistic(fetchstatistic(url=url,
                                                 next_url=None,
                                                 status=None,
                                                 exception=exception,
                                                 size=0,
                                                 content_type=None,
                                                 encoding=None,
                                                 new_urls=set(),
                                                 body=None))
            return

        try:
            if is_redirect(response):
                location = response.headers['location']
                next_url = urllib.parse.urljoin(url, location)
                self.record_statistic(fetchstatistic(url=url,
                                                     next_url=next_url,
                                                     status=response.status,
                                                     exception=None,
                                                     size=0,
                                                     content_type=None,
                                                     encoding=None,
                                                     new_urls=set(),
                                                     body=None))
                if next_url in self.seen_urls:
                    return
                if max_redirect > 0:
                    logger.info('redirect to %r from %r', next_url, url)
                    self.add_url(next_url, max_redirect - 1)
                else:
                    logger.error('redirect limit reached for %r from %r',
                                 next_url, url)
            else:
                stat, links = yield from self.parse_links(response)
                self.record_statistic(stat)
                self.seen_urls.update(links)
        finally:
            yield from response.release()

   
    @asyncio.coroutine 
    def work(self):
        """Process queue items forever."""
        try:
            while True:
                url, max_redirect = yield from self.q.get()
                yield from self.fetch(url, max_redirect)
                self.q.task_done()
        except asyncio.CancelledError:
            pass

    def url_allowed(self, url):
        if self.exclude and re.search(self.exclude, url):
            return False
        parts = urllib.parse.urlparse(url)
        if parts.scheme not in ('http', 'https'):
            logger.debug('skipping non-http scheme in %r', url)
            return False
        host, port = urllib.parse.splitport(parts.netloc)
        if not self.host_okay(host):
            logger.debug('skipping non-root host in %r', url)
            return False
        return True

    def add_url(self, url, max_redirect=None):
        """Add a URL to the queue if not seen before."""
        if max_redirect is None:
            max_redirect = self.max_redirect
        logger.debug('adding %r %r', url, max_redirect)
        self.seen_urls.add(url)
        self.q.put_nowait((url, max_redirect))

    
    @asyncio.coroutine
    def crawl(self):
        """Run the crawler until all finished."""
        workers = [asyncio.Task(self.work(), loop=self.loop)
                   for _ in range(self.max_tasks)]
        self.t0 = time.time()
        yield from self.q.join()
        self.t1 = time.time()
        for w in workers:
            w.cancel()

            
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=settings.FORMAT)
    parser = argparse.ArgumentParser()
    parser.add_argument('--host',
                        help='remote host addr: ip:port',
                        action='store')
    args = parser.parse_args()
    ip, port = args.host.split(':')
    port = int(port)
    proxy = ServerProxy('http://{}:{}/'.format(ip, port), allow_none=True)
    loop = asyncio.get_event_loop() 
    pid = os.getpid()
    cr = None
    try:
        while True: 
            try:           
                tasks = proxy.get_tasks(pid, settings.CRAWLER_NUM)
            except OSError as e:
                logger.info('cannot connect to master')
                time.sleep(settings.CONNECTIONREFUSED_SLEEP)
                continue
            if len(tasks) == 0:
                logger.info('no more tasks!')
                time.sleep(settings.NO_TASKS_SLEEP)
                continue
            logger.info('recv: {} tasks'.format(len(tasks)))
            cr = Crawler(tasks)
            loop.run_until_complete(cr.crawl())
            # if len(cr.fail_done) > 0:
            #     proxy.send_failed_results(pid, cr.fail_done)
            # time.sleep(2)
            logger.info("{} tasks done!".format(len(tasks)))
            try:
                proxy.send_results(pid, cr.done)
            except OSError as e:
                logger.warn(str(e))
            except Exception as e:
                logger.warn(str(e))
            logger.info('sent done')
            cr.close()
            
    finally:
        if cr is not None:
            cr.close()
        loop.stop()
        loop.run_forever()
        loop.close()
    