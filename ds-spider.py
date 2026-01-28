import os
import queue
import requests
import sys
import threading
from io       import BytesIO
from ds_store import DSStore

try: 
    from urllib.parse import urlparse
except Exception:
    from urlparse import urlparse



class DSSpider(object):

    DIRECTORY = os.path.abspath('.')



    def __init__(self):
        url = self._get_args()

        self._queue = queue.Queue()
        self._queue.put(url)
        
        self._processed_url   = set()
        self._lock            = threading.Lock()
        self._working_threads = 0

        self._running = True


    
    @staticmethod
    def _get_args():
        if len(sys.argv) <= 1:
            sys.exit(1)

        return sys.argv[1]


    
    def _display(self, msg):
        self._lock.acquire()
        print(msg)
        self._lock.release()

    

    def scan(self):
        all_threads = []
        for _ in range(10):
            t = threading.Thread(target=self._process)
            all_threads.append(t)
            t.start()



    def _process(self):
        while self._running:
            try:
                url = self._get_url()
            except queue.Empty:
                self._check_threads()
                continue
            except Exception:
                self._display(f'[ERROR] _get_url: {e}')
                continue                

            if url in self._processed_url:
                continue
            
            url, base_url            = self._parse_url(url)
            _, netloc, path, _, _, _ = urlparse(url, 'http')    
            response                 = self._download(url)

            if response is None or response.status_code != 200:
                self._working_threads -= 1
                continue

            try:
                self._create_folder(netloc, path)
                self._save_file(netloc, path, response, url)
                
                if not url.endswith('.DS_Store'):
                    continue
                
                self._process_ds_store_file(response, base_url)

            except Exception as e:
                self._display('[ERROR] %s' % str(e))
            finally:
                self._working_threads -= 1

    

    def _get_url(self):
        try:
            url = self._queue.get(timeout=2.0)
            self._lock.acquire()
            self._working_threads += 1
            self._lock.release()
            return url
        except queue.Empty:
            raise
        except Exception as e:
            self._display(f'[ERROR] queue.get: {e}')
            raise
    


    def _check_threads(self):
        self._lock.acquire()
        
        if self._working_threads == 0:
            self._running = False
        
        self._lock.release()
    


    def _parse_url(self, url):
        self._processed_url.add(url)
        base_url = url.rstrip('.DS_Store')
                
        if not url.lower().startswith('http'):
            url = 'http://%s' % url
        
        return url, base_url
    


    def _download(self, url):
        try:
            return requests.get(url, allow_redirects=False)
        except Exception as e:            
            self._display('[ERROR] %s' % str(e))            
            return None
    


    def _create_folder(self, netloc, path):
        folder_name = netloc.replace(':', '_') + '/'.join(path.split('/')[:-1])
        
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

    

    def _save_file(self, netloc, path, response, url):
        with open(netloc.replace(':', '_') + path, 'wb') as out_file:
            self._display('[%s] %s' % (response.status_code, url))
            out_file.write(response.content)
    


    def _process_ds_store_file(self, response, base_url):
        ds_store_file = BytesIO()
        ds_store_file.write(response.content)
        
        d = DSStore.open(ds_store_file)
        dirs_files = set()
                
        for x in d._traverse(None):
            if self._is_valid_name(x.filename):
                dirs_files.add(x.filename)
                
        for name in dirs_files:
            if name == '.':
                continue
                
            self._queue.put(base_url + name)
    
            if len(name) > 4 and name[-4] == '.':
                continue

            self._queue.put(base_url + name + '/.DS_Store')
                
        d.close()



    def _is_valid_name(self, entry_name):
        validation = entry_name.find('..') >= 0 or entry_name.startswith('/')
        validation = validation or entry_name.startswith('\\')
        validation = validation or not os.path.abspath(entry_name).startswith(self.DIRECTORY)
        
        if validation:
            try:
                print('[ERROR] Invalid entry name: %s' % entry_name)
            except Exception:
                pass

            return False
        
        return True





if __name__ == '__main__':
    s = DSSpider()
    s.scan()
