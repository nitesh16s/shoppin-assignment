import re
import json
import asyncio
import aiohttp
import logging
import aiofiles
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import defaultdict

class Crawler:

    def  __init__(self):
        self.timeout = 10
        self.max_retries = 5
        self.failed_urls = 0
        self.visited_urls = set()
        self.product_urls = defaultdict(set)
        self.total_urls_crawled = 0
        self.session = None
        self.setup_logging()

    def setup_logging(self):
        """logger setup"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    async def init_session(self):
        """async session for the crawler"""
        if not self.session:
            self.session = aiohttp.ClientSession(
            headers={'User-Agent': 'Mozilla/5.0 (compatible; ProductCrawler/1.0;)'}
        )

    def get_domain(self, url):
        """get domain from link"""
        return urlparse(url).netloc

    def extract_urls(self, html, base_url):
        """Parses the html and extract all the href links"""
        soup = BeautifulSoup(html, 'html.parser')
        urls = set()

        for anchor in soup.find_all('a', href=True):
            url = urljoin(base_url, anchor['href'])
            if url.startswith(('http://', 'https://')):
                urls.add(url)
        return urls

    def is_product_url(self, url) -> bool:
        """configure it accordingly to the website based on the pdp"""
        patterns = [
            r'/product',   #hyugalife, themomosco, plix, dermaco
            r'/products',    #plum, mcaffeine, dermatouch, mamaearth, discoverpilgrim
            r'/collections',    #beminamlist
            r'/items'
        ]
        return any(re.search(pattern, url.lower()) for pattern in patterns)

    async def fetch_html(self, url):
        """get the html page for a link"""
        for attempt in range(self.max_retries):
            try:
                async with self.session.get(url, allow_redirects=True, timeout=self.timeout) as response:
                    if response.status == 200:
                        return await response.text()
                    return None
            except Exception as e:
                self.logger.error(f"Failed to fetch {url}: {e}")
                if attempt == self.max_retries - 1:
                    self.failed_urls += 1
                    return None
                await asyncio.sleep(attempt+1)
        return None

    async def crawl_url(self, url, domain):
        """
        function to crawl given url from a domain.
        checks for the links if already crawled
        maintain stats
        """

        # if we have seen that url already
        if url in self.visited_urls:
            return
        
        # add url to visited urls set
        self.visited_urls.add(url)
        self.total_urls_crawled += 1

        # logging: how many urls crawled
        if self.total_urls_crawled % 100 == 0:
            self.logger.info(f"Crawled {self.total_urls_crawled} URLs")

        # returns html data from a url
        html = await self.fetch_html(url)
        if not html:
            return
        
        # returns whether a url mathches or not the pdp path
        if self.is_product_url(url):
            self.product_urls[domain].add(url)

        # set of urls, from html data
        urls = self.extract_urls(html, url)
        # to keep track of same domain urls
        same_domain_urls = {url for url in urls if self.get_domain(url) == domain}

        # create one coroutine per url, run coroutines once count is 10
        tasks = []
        # crawling url if not visited
        for new_url in same_domain_urls - self.visited_urls:
            if len(tasks) > 10:
                await asyncio.gather(*tasks)
                tasks = []
            tasks.append(asyncio.create_task(self.crawl_url(url=new_url, domain=domain)))

        if tasks:
            await asyncio.gather(*tasks)

    async def save_results(self):
        """add crawled products to a file"""
        results = {'products': {}, 'failed_urls': self.failed_urls}
        for domain, urls in self.product_urls.items():
            results['products'][domain] = list(urls)

        async with aiofiles.open('results.json', 'w') as f:
            await f.write(json.dumps(results, indent=2))

async def main(domains):
    """
    entry point to start crawling for domain asynchronously
    """
    crawler = Crawler()
    await crawler.init_session()
    tasks = []
    for domain in domains:
        crawler.logger.info(f"Starting crawl of domain: {domain}")
        url = f'https://{domain}'
        tasks.append(asyncio.create_task(crawler.crawl_url(url, domain)))
        
    await asyncio.gather(*tasks)
    await crawler.session.close()
    await crawler.save_results()
    
# list of domains
domains = [
    'hyugalife.com',
    'thedermaco.com',
    'mamaearth.in',
    'themomsco.com',
    'mcaffeine.com',
    'dermatouch.com',
    'beminimalist.co',
    'plixlife.com',
    'discoverpilgrim.com'
]

asyncio.run(main(domains))
