import os
import shelve

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

from urllib.parse import urlparse

class Unique(object):
    def __init__(self, config, restart):
        self.logger = get_logger("UNIQUE")
        self.config = config
        self.count = 0
        
        unique_shelf = config.cparser.get('LOCAL PROPERTIES', 'SAVE').split(',')[1].strip()
        if not os.path.exists(unique_shelf) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find unique save file {unique_shelf}, "
                f"starting from seed.")
        elif os.path.exists(unique_shelf) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found unique save file {unique_shelf}, deleting it.")
            os.remove(unique_shelf)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(unique_shelf)
        if restart:
            for url in self.config.seed_urls:
                self.add_if_unique(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_if_unique(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        self.count = total_count

    def add_if_unique(self, url):
        url = normalize(url)
        url = self.extract_url_without_fragment(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = url
            self.save.sync()
            self.count += 1
    
    def extract_url_without_fragment(self, url):
        parsed_url = urlparse(url)
        url_without_fragment = parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path
        if parsed_url.query:
            url_without_fragment += "?" + parsed_url.query
        return url_without_fragment
