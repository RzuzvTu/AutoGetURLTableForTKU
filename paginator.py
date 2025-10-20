import re
import time
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)

class Paginator:
    """自動分頁抓取管理器，支援 spid 和 pg 切換"""
    
    def __init__(self, crawler, parser):
        self.crawler = crawler
        self.parser = parser
        self.session = getattr(crawler, 'session', None)
    
    def auto_fetch_all_pages(self, base_url: str, container_selector: str, 
                           max_pages: Optional[int] = None, table_mode: bool = False,
                           pagination_config: Optional[Dict] = None) -> List[Dict]:
        """自動抓取所有頁面資料"""
        print(f"自動分頁抓取: {base_url}")
        
        # 1. 抓取第一頁並偵測分頁
        print("分析第一頁分頁結構...")
        first_page_soup = self.crawler.fetch(base_url)
        if not first_page_soup:
            print("第一頁抓取失敗")
            return []
        
        pagination_info = self.parser.detect_pagination(first_page_soup)
        total_pages = min(pagination_info['total_pages'], max_pages or 10)
        
        print(f"偵測到 {total_pages} 頁 (最多抓取 {max_pages or '無限制'} 頁)")
        
        if total_pages == 1:
            print("只有一頁，使用單頁解析")
            return self.parser.parse(first_page_soup, container_selector, table_mode)
        
        # 2. 生成頁面URL
        page_urls = self._generate_page_urls(base_url, total_pages, pagination_info, pagination_config)
        
        # 3. 抓取所有頁面
        all_data = []
        for i, page_url in enumerate(page_urls, 1):
            print(f"抓取第 {i}/{min(total_pages, len(page_urls))} 頁: {page_url[-50:]}")
            
            page_soup = self.crawler.fetch(page_url)
            if not page_soup:
                print(f"第{i}頁抓取失敗")
                continue
            
            page_data = self.parser.parse(page_soup, container_selector, table_mode)
            all_data.extend(page_data)
            print(f"本頁解析 {len(page_data)} 筆資料")
            
            # 延遲避免被封鎖
            if i < total_pages:
                time.sleep(1)
        
        # 4. 去重複
        unique_data = self._deduplicate_data(all_data)
        print(f"總計 {len(unique_data)} 筆唯一資料")
        
        return unique_data
    
    def _generate_page_urls(self, base_url: str, total_pages: int, 
                          pagination_info: Dict, pagination_config: Optional[Dict] = None) -> List[str]:
        """生成分頁URL，支援 spid 和 pg 動態切換"""
        urls = []
        parsed_url = urlparse(base_url)
        base_path = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
        query_params = parse_qs(parsed_url.query)
        
        # 提取 spid
        spid = query_params.get('spid', [None])[0]
        if not spid:
            logger.warning("未找到 spid 參數，可能影響分頁正確性")
        
        # 使用 pagination_config 或預設 page_param
        page_param = (pagination_config.get('page_param', 'pg') 
                     if pagination_config else pagination_info.get('page_param', 'pg'))
        
        for page in range(1, total_pages + 1):
            # 第一頁保留 spid
            if page == 1 and spid:
                page_query = {'spid': spid}
            else:
                # 後續頁面使用 pg 並保留 spid
                page_query = {'pg': str(page)}
                if spid:
                    page_query['spid'] = spid
            
            url = f"{base_path}?{urlencode(page_query, doseq=True)}"
            urls.append(url)
        
        logger.info(f"生成 {len(urls)} 個分頁URL: {urls[:2]}...")
        return urls
    
    def _deduplicate_data(self, data_list: List[Dict]) -> List[Dict]:
        """去重複資料（使用 nid 或 hash）"""
        seen_nids = set()
        seen_hashes = set()
        unique_data = []
        
        for item in data_list:
            identifier = item.get('nid') or item.get('hash')
            if identifier and identifier not in seen_nids and identifier not in seen_hashes:
                seen_nids.add(item.get('nid'))
                seen_hashes.add(item.get('hash'))
                unique_data.append(item)
        
        return unique_data