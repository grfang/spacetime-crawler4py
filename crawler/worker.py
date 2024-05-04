from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen
from urllib.robotparser import RobotFileParser
import socket
from bs4 import BeautifulSoup


class Worker(Thread):
    def __init__(self, worker_id, config, frontier, unique, subdomains):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # shelve to keep track of unique urls
        self.unique = unique
        # shelve to keep track of unique subdomains and count
        self.subdomains = subdomains
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        # List to store fingerprints for future comparison
        final_lst = []
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break

            try:
                # Checking robots to see if crawling allowed
                parsed_url = urlparse(tbd_url)
                base_url = parsed_url.scheme + "://" + parsed_url.netloc
                if not self.robot_allowed(base_url):
                    self.logger.info(f"Skipping {tbd_url} due to robots.txt rules")
                    self.frontier.mark_url_complete(tbd_url)
                    continue
            except:
                pass

            # Getting the depth of the current url and adding 1
            depth = self.frontier.get_depth(tbd_url) + 1
            # # If depth of url is greater than 120, probably a trap, so skip
            # if depth > 120:
            #     self.logger.info(f"Skipping {tbd_url} due to depth limit")
            #     self.frontier.mark_url_complete(tbd_url)
            #     continue

            try:
                # Download url
                resp = download(tbd_url, self.config, self.logger)
                # if error, try to download again, max 5 times before moving on
                if not resp or not resp.raw_response:
                    for _ in range(5):
                        resp = download(tbd_url, self.config, self.logger)
                        if resp  and resp.raw_response:
                            break
                if not resp or not resp.raw_response:
                    self.frontier.mark_url_complete(tbd_url)
                    continue
            except:
                self.frontier.mark_url_complete(tbd_url)
                continue

            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>., "
                f"using cache {self.config.cache_server}.")
            
            # Scraped urls
            scraped_urls, final_lst = scraper.scraper(tbd_url, resp, final_lst)

            # If the url was scraped, add to unique list and unique subdomains list if new
            if len(scraped_urls) > 0:
                self.unique.add_if_unique(tbd_url)
                self.subdomains.add_if_new_subdomain(tbd_url)

            # Add new urls to frontier
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url, depth)
            self.frontier.mark_url_complete(tbd_url)

            # # Check for other possible urls in sitemaps
            # sitemap_urls = self.check_and_process_sitemap(base_url)
            # for sitemap_url in sitemap_urls:
            #     self.frontier.add_url(sitemap_url, depth)

            # Time delay for politeness
            time.sleep(self.config.time_delay)
        # Log info
        self.logger.info(f"Number of Unique Pages: {self.unique.count}")
        self.logger.info(f"Number of subdomains: {self.subdomains.count}")

    def fetch_robots(self, url):
        # Fetch robots.txt if one exists
        parser = RobotFileParser()
        robots_url = urljoin(url, '/robots.txt')
        try:
            socket.setdefaulttimeout(3)
            parser.set_url(robots_url)
            parser.read()
            return parser
        except socket.timeout:
            self.logger.warning("Timeout occurred while fetching robots.txt")
            return None
        except Exception as e:
            self.logger.warning(f"Failed to fetch or parse robots.txt for {url}: {e}")
            return None
        finally:
            socket.setdefaulttimeout(None)

    def robot_allowed(self, url):
        robot = self.fetch_robots(url)
        if robot:
            # If robots.txt found and parsed, check if crawling is allowed
            return robot.can_fetch("*", url)
        # If robots.txt not found, allow crawling
        return True
    
    def check_and_process_sitemap(self, url):
        robots_url = urljoin(url, '/robots.txt')
        try:
            # Try to get robots
            with urlopen(robots_url) as response:
                robots_content = response.read().decode('utf-8')
            sitemap_url = None
            # Parse robots.txt file and find sitemap URL
            for line in robots_content.split('\n'):
                if line.startswith('Sitemap:'):
                    sitemap_url = line.split(': ')[1].strip()
                    break
            # If sitemaps exist - process it, otherwise log not found
            if sitemap_url:
                return self.process_sitemap(sitemap_url)
            else:
                self.logger.info("No sitemap found in robots.txt")
        except Exception as e:
            self.logger.error(f"Error while checking sitemap: {e}")

    def process_sitemap(self, sitemap_url):
        try:
            sitemap_links = []
            # Parse the sitemap XML file
            with urlopen(sitemap_url) as response:
                sitemap_content = response.read().decode('utf-8')
            soup = BeautifulSoup(sitemap_content, 'xml')
            # Extract URLs and add them to list
            urls = soup.find_all('url')
            for url in urls:
                loc = url.find('loc').text.strip()
                if scraper.is_valid(loc):
                    sitemap_links.append(loc)
                    self.logger.info(f"Added {loc} from sitemap")
            return sitemap_links
        except Exception as e:
            self.logger.error(f"Error while processing sitemap: {e}")
