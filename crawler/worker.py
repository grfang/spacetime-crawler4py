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
        self.unique = unique
        self.subdomains = subdomains
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        final_lst = []
        archive_count = 0
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            if 'archive.ics.uci.edu' in tbd_url:
                if archive_count > 5000:
                    print(f"NOT PROCESSING {tbd_url} because is it archive and count={archive_count}")
                    self.frontier.mark_url_complete(tbd_url)
                    continue
                else:
                    print("archive: ", archive_count)
                    archive_count += 1
            try:
                parsed_url = urlparse(tbd_url)
                base_url = parsed_url.scheme + "://" + parsed_url.netloc
                if not self.robot_allowed(base_url):
                    self.logger.info(f"Skipping {tbd_url} due to robots.txt rules")
                    self.frontier.mark_url_complete(tbd_url)
                    continue
            except:
                pass
            depth = self.frontier.get_depth(tbd_url) + 1
            print(tbd_url, depth)
            # if depth > 30:
            #     self.logger.info(f"Skipping {tbd_url} due to depth limit")
            #     self.frontier.mark_url_complete(tbd_url)
            #     continue
            try:
                resp = download(tbd_url, self.config, self.logger)
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
            scraped_urls, final_lst = scraper.scraper(tbd_url, resp, final_lst)
            #scraped_urls = scraper.scraper(tbd_url, resp)
            if len(scraped_urls) > 0:
                self.unique.add_if_unique(tbd_url)
                self.subdomains.add_if_new_subdomain(tbd_url)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url, depth)
            self.frontier.mark_url_complete(tbd_url)
            # sitemap_urls = self.check_and_process_sitemap(base_url)
            # for sitemap_url in sitemap_urls:
            #     self.frontier.add_url(sitemap_url, depth)
            time.sleep(self.config.time_delay)
        self.logger.info(f"Number of Unique Pages: {self.unique.count}")
        self.logger.info(f"Number of subdomains: {self.subdomains.count}")

    def fetch_robots(self, url):
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
            return robot.can_fetch("*", url)
        return True
    
    def check_and_process_sitemap(self, url):
        robots_url = urljoin(url, '/robots.txt')
        try:
            with urlopen(robots_url) as response:
                robots_content = response.read().decode('utf-8')
            sitemap_url = None
            for line in robots_content.split('\n'):
                if line.startswith('Sitemap:'):
                    sitemap_url = line.split(': ')[1].strip()
                    break
            if sitemap_url:
                return self.process_sitemap(sitemap_url)
            else:
                self.logger.info("No sitemap found in robots.txt")
        except Exception as e:
            self.logger.error(f"Error while checking sitemap: {e}")

    def process_sitemap(self, sitemap_url):
        try:
            sitemap_links = []
            with urlopen(sitemap_url) as response:
                sitemap_content = response.read().decode('utf-8')
            soup = BeautifulSoup(sitemap_content, 'xml')
            urls = soup.find_all('url')
            for url in urls:
                loc = url.find('loc').text.strip()
                if scraper.is_valid(loc):
                    sitemap_links.append(loc)
                    self.logger.info(f"Added {loc} from sitemap")
            return sitemap_links
        except Exception as e:
            self.logger.error(f"Error while processing sitemap: {e}")
