import os
import requests
import sys
import time
from threading    import Lock, Thread
from queue        import Queue, Empty
from io           import BytesIO
from ds_store     import DSStore
from urllib.parse import urlparse



class DSSpider:

    def __init__(self):
        url = self._get_args()

        self._queue: Queue = Queue()
        self._queue.put(url)
        
        self._lock:           Lock = Lock()
        self._threads:        int  = 0
        self._DIR:            str  = os.path.abspath('.')
        self._processed_urls: set  = set()



    
    @staticmethod
    def _get_args() -> str:
        if len(sys.argv) <= 1:
            print('[ ERROR ] Missing argument/URL')
            print('[ USAGE ] python ds-spider.py http://example.com/.DS_Store')
            sys.exit(1)

        return sys.argv[1]
    


    def run(self):
        print('[+] Crawler started\n')
        self._add_thread()
        
        while self._threads > 0:
            time.sleep(0.5)
        
        print('\n[-] Crawler finished')


    
    def _add_thread(self):
        if self._threads >= 10:
            return

        with self._lock:
            self._threads += 1
            t = Thread(target=self._scan)
            t.daemon = False
            t.start()


    
    def _remove_thread(self):
        with self._lock: 
            self._threads -= 1



    def _scan(self):        
        while True:
            url = self._get_url()

            if not url:
                self._remove_thread()
                break

            if url in self._processed_urls:
                continue
            
            url, base_url = self._parse_url(url)
            netloc, path  = self._split_url(url)    
            response      = self._download(url)

            if response is None or response.status_code != 200:
                continue

            self.processes_response(netloc, path, response, url, base_url)



    def _get_url(self) -> str | None:
        try:
            url = self._queue.get(timeout=2.0)
            return url
        except Empty:
            return None
        except Exception as e:
            self._display(f'[ ERROR ] queue.get: {e}')
            return None
    


    def _split_url(self, url) -> tuple[str]:
        _, netloc, path, _, _, _ = urlparse(url, 'http')
        return netloc, path

    

    def _display(self, msg):
        with self._lock:
            print(msg)
            


    def _parse_url(self, url):
        self._processed_urls.add(url)
        base_url = url.rstrip('.DS_Store')
                
        if not url.lower().startswith('http'):
            url = 'http://%s' % url
        
        return url, base_url
    


    def _download(self, url):
        try:
            return requests.get(url, allow_redirects=False, timeout=10)
        except Exception as e:            
            self._display('[ ERROR ] %s' % str(e))            
            return None
    


    def processes_response(self, netloc, path, response, url, base_url):
        try:
            self._create_folder(netloc, path)
            self._save_file(netloc, path, response, url)
                
            if not url.endswith('.DS_Store'):
                return
                
            self._process_ds_store_file(response, base_url)

        except Exception as e:
            self._display('[ ERROR ] %s' % str(e))



    def _create_folder(self, netloc, path):
        folder_name = netloc.replace(':', '_') + '/'.join(path.split('/')[:-1])
        
        if not os.path.exists(folder_name):
            os.makedirs(folder_name, exist_ok=True)

    

    def _save_file(self, netloc, path, response, url):
        filename = netloc.replace(':', '_') + path
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'wb') as out_file:
            self._display(f'[{response.status_code}] {url}')
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
            self._add_thread()
    
            if '.' not in name or len(name.split('.')[-1]) > 4:
                self._queue.put(base_url + name + '/.DS_Store')
                self._add_thread()
                
        d.close()



    def _is_valid_name(self, entry_name):
        validation = entry_name.find('..') >= 0 or entry_name.startswith('/')
        validation = validation or entry_name.startswith('\\')
        validation = validation or not os.path.abspath(entry_name).startswith(self._DIR)
        
        if validation:
            try:
                print('[ ERROR ] Invalid entry name: %s' % entry_name)
            except Exception:
                pass

            return False
        
        return True





if __name__ == '__main__':
    spider = DSSpider()
    spider.run()
