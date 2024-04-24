from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import socket


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        final_lst = []
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            parsed_url = urlparse(tbd_url)
            base_url = parsed_url.scheme + "://" + parsed_url.netloc
            if not self.robot_allowed(base_url):
                self.logger.info(f"Skipping {tbd_url} due to robots.txt rules")
                continue
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>., "
                f"using cache {self.config.cache_server}.")
            scraped_urls, final_lst = scraper.scraper(tbd_url, resp, final_lst)
            # print("David test")
            # print(scraped_urls)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)

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
