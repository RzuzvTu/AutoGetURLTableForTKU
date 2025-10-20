import requests
from bs4 import BeautifulSoup
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class WebCrawler:
    def __init__(self, headers, timeout=10, retry_times=3):
        self.headers = headers
        self.timeout = timeout
        self.retry_times = retry_times
        self.session = self._create_session()
    
    def _create_session(self):
        """建立帶重試機制的session"""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.retry_times,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def fetch(self, url):
        """抓取單一網頁"""
        try:
            print(f"正在抓取: {url}")
            response = self.session.get(
                url, 
                headers=self.headers, 
                timeout=self.timeout
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            print(f"抓取成功: {len(soup.select('body')) > 0}")
            time.sleep(random.uniform(1, 3))
            return soup
        except requests.exceptions.RequestException as e:
            print(f"抓取失敗 {url}: {e}")
            return None
        except Exception as e:
            print(f"解析失敗 {url}: {e}")
            return None
    
    def close(self):
        """關閉session"""
        self.session.close()