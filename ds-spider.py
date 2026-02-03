import os
import sys
import time
import requests
from dataclasses  import dataclass
from threading    import Lock, Thread
from queue        import Queue, Empty
from io           import BytesIO
from ds_store     import DSStore
from urllib.parse import urlparse



@dataclass(slots=True)
class Data:
    url:      str = None
    base_url: str = None
    netloc:   str = None
    path:     str = None
    response: requests.Response = None



class DSSpider:

    def __init__(self):
        self._queue:        Queue = Queue()
        self._lock:          Lock = Lock()
        self._threads:        int = 0
        self._DIR:            str = os.path.abspath('.')
        self._processed_urls: set = set()


    
    def run(self):
        print('[+] Crawler started\n')
        self._get_args()
        
        while self._threads > 0:
            time.sleep(0.5)
        
        print('\n[-] Crawler finished')


    
    def _get_args(self) -> str:
        if len(sys.argv) <= 1:
            print('[ ERROR ] Missing argument/URL')
            print('[ USAGE ] python ds-spider.py http://example.com/.DS_Store')
            sys.exit(1)

        self._enqueue_url(sys.argv[1])

    

    def _enqueue_url(self, url: str):
        if url in self._processed_urls:
            return
        
        self._queue.put(url)
        self._add_thread()


    
    def _add_thread(self):
        if self._threads >= 10:
            return

        with self._lock:
            self._threads += 1
            t = Thread(target=self._scan)
            t.daemon = False
            t.start()



    def _display(self, msg: str):
        with self._lock:
            print(msg)



    def _scan(self):        
        while True:
            url = self._get_url()

            if not url:
                self._remove_thread()
                break
            
            self._add_processed_url(url)
            
            data     = Data()
            data.url = url
            
            self._parse_url(data)
            self._split_url(data)    
            self._download(data)

            if data.response is None or data.response.status_code != 200:
                continue
            
            self._processes_response(data)



    def _get_url(self) -> str | None:
        try:
            url = self._queue.get(timeout=2.0)
            return url
        except Empty:
            return None
        except Exception as e:
            self._display(f'[!] queue.get: {e}')
            return None
        
    

    def _remove_thread(self):
        with self._lock: 
            self._threads -= 1

    

    def _add_processed_url(self, url: str):
        with self._lock:
            self._processed_urls.add(url)
    


    def _parse_url(self, data: Data):
        data.base_url = data.url.rstrip('.DS_Store')
                
        if not data.url.lower().startswith('http'):
            data.url = f'http://{data.url}'
    


    def _split_url(self, data: Data):
        _, netloc, path, _, _, _ = urlparse(data.url, 'http')
        data.netloc = netloc
        data.path   = path
    


    def _download(self, data: Data):
        try:
            data.response = requests.get(data.url, allow_redirects=False, timeout=10)
        except Exception as e:            
            self._display('[!] %s' % str(e))            
            data.response = None
    


    def _processes_response(self, data: Data):
        try:
            self._create_folder(data)
            self._save_file(data)
                
            if not data.url.endswith('.DS_Store'):
                return
                
            self._process_ds_store_file(data)

        except Exception as e:
            self._display(f'[!] {str(e)}')



    @staticmethod
    def _create_folder(data: Data):
        folder_name = data.netloc.replace(':', '_') + '/'.join(data.path.split('/')[:-1])
        
        if not os.path.exists(folder_name):
            os.makedirs(folder_name, exist_ok=True)

    

    def _save_file(self, data: Data):
        filename = data.netloc.replace(':', '_') + data.path
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'wb') as out_file:
            self._display(f'[{data.response.status_code}] {data.url}')
            out_file.write(data.response.content)
    


    def _process_ds_store_file(self, data: Data):
        with (BytesIO(data.response.content) as dss_file, DSStore.open(dss_file) as d):
            dirs_files = set()

            for entry in d._traverse(None):
                if self._is_valid_name(entry.filename):
                    dirs_files.add(entry.filename)

            for name in dirs_files:
                if name == '.':
                    continue

                self._enqueue_url(data.base_url + name)

                if '.' not in name or len(name.split('.')[-1]) > 4:
                    self._enqueue_url(data.base_url + name + '/.DS_Store')




    def _is_valid_name(self, entry_name: str) -> bool: 
        is_invalid = entry_name.find('..') >= 0
        is_invalid = is_invalid or entry_name.startswith('/')
        is_invalid = is_invalid or entry_name.startswith('\\')
        is_invalid = is_invalid or not os.path.abspath(entry_name).startswith(self._DIR)
        
        return not is_invalid





if __name__ == '__main__':
    spider = DSSpider()
    spider.run()
