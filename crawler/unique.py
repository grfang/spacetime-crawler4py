import os
import shelve

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

from urllib.parse import urlparse

class Unique(object):
    ''' Same functionality as frontier. In shelve: stores url hashes as keys and urls as values. '''
    def __init__(self, config, restart):
        self.logger = get_logger("UNIQUE")
        self.config = config
        self.count = 0
        
        if not os.path.exists(self.config.unique_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find unique save file {self.config.unique_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.unique_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found unique save file {self.config.unique_file}, deleting it.")
            os.remove(self.config.unique_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.unique_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_if_unique(url)
        else:
            # Set the unique state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_if_unique(url)

    def _parse_save_file(self):
        ''' Restores the total count value '''
        total_count = len(self.save)
        self.count = total_count

    def add_if_unique(self, url):
        ''' Gets the url without the fragment and stores it in shelve if it doesn't already exist in shelve. '''
        url = normalize(url)
        url = self.extract_url_without_fragment(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = url
            self.save.sync()
            self.count += 1
    
    def extract_url_without_fragment(self, url):
        ''' Extracts the url without the fragment. '''
        parsed_url = urlparse(url)
        url_without_fragment = parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path
        if parsed_url.query:
            url_without_fragment += "?" + parsed_url.query
        return url_without_fragment
