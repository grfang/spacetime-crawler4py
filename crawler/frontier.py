import os
import shelve

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        
        frontier_shelf = config.cparser.get('LOCAL PROPERTIES', 'SAVE').split(',')[0].strip()
        if not os.path.exists(frontier_shelf) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {frontier_shelf}, "
                f"starting from seed.")
        elif os.path.exists(frontier_shelf) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {frontier_shelf}, deleting it.")
            os.remove(frontier_shelf)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(frontier_shelf)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url, 0)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url, 0)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed, depth in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        try:
            return self.to_be_downloaded.pop()
        except IndexError:
            return None

    def add_url(self, url, depth):
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = (url, False, depth)
            self.save.sync()
            self.to_be_downloaded.append(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        self.save[urlhash] = (url, True, self.save[urlhash][2])
        self.save.sync()

    def get_depth(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Trying to get depth of url {url}, but have not seen it before.")

        return self.save[urlhash][2]
