import os
import shelve

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

from urllib.parse import urlparse

class Subdomains(object):
    def __init__(self, config, restart):
        self.logger = get_logger("SUBDOMAINS")
        self.config = config
        self.count = 0
        
        if not os.path.exists(self.config.subdomain_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find subdomain save file {self.config.subdomain_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.subdomain_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found subdomain save file {self.config.subdomain_file}, deleting it.")
            os.remove(self.config.subdomain_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.subdomain_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_if_new_subdomain(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_if_new_subdomain(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        self.count = total_count

    def add_if_new_subdomain(self, url):
        url = normalize(url)
        parsed_url = urlparse(url)
        base_url = parsed_url.scheme + "://" + parsed_url.netloc
        urlhash = get_urlhash(base_url)
        if urlhash not in self.save:
            self.save[urlhash] = (base_url, 1)
            self.save.sync()
            self.count += 1
        else:
            num = self.save[urlhash][1] + 1
            self.save[urlhash] = (base_url, num)
            self.save.sync()
