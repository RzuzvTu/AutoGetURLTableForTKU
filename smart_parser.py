import hashlib
import re
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from bs4 import BeautifulSoup, Tag

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SmartParser:
    """
    HTML解析器 - 淡江大學深耕網專用
    """
    
    def __init__(self, max_items: int = 100, table_mode: bool = False):
        self.max_items = max_items
        self.table_mode = table_mode
        self.field_patterns = self._get_field_patterns()
        self.pagination_cache = {}
    
    def _get_field_patterns(self) -> Dict[str, List]:
        """定義欄位偵測模式（僅用於一般模式）"""
        return {
            'title': [
                ('h1', 'h2', 'h3', 'h4', 'h5'),
                ('.title', '.headline', '.name', '[class*="title"]', '[class*="head"]', '[class*="news"]'),
                ('a', 'strong', '[id*="title"]', '[id*="news"]'),
                lambda elem: len(elem.get_text(strip=True)) > 5
            ],
            'link': [
                ('a[href]', 'a', '[href]'),
                lambda elem: elem.name == 'a' and elem.get('href')
            ],
            'date': [
                ('time', '.date', '.published', '.time', '[class*="date"]', '[class*="time"]', '[class*="publish"]'),
                lambda elem: re.search(
                    r'\d{4}[/-]\d{1,2}[/-]\d{1,2}|\d{1,2}[/-]\d{1,2}[/-]\d{4}',
                    elem.get_text(strip=True)
                )
            ]
        }
    
    def parse(self, soup: BeautifulSoup, container_selector: str, 
              table_mode: Optional[bool] = None) -> List[Dict]:
        """主解析方法（標題、日期、連結）"""
        if not soup:
            logger.warning("空的Soup物件")
            return []
        
        auto_table_mode = table_mode or self._is_table_mode(container_selector)
        logger.info(f"使用{'表格模式' if auto_table_mode else '一般模式'}: {container_selector}")
        
        containers = soup.select(container_selector)
        logger.info(f"找到 {len(containers)} 個資料容器")
        
        if not containers:
            logger.warning(f"容器選擇器 '{container_selector}' 未找到元素")
            return []
        
        data_list = self._parse_table_mode(containers) if auto_table_mode else self._parse_general_mode(containers)
        
        valid_data = self.validate_data(data_list)
        logger.info(f"最終有效資料: {len(valid_data)} 筆")
        
        return valid_data[:self.max_items]
    
    def _is_table_mode(self, selector: str) -> bool:
        """判斷表格模式"""
        return any(indicator in selector.lower() for indicator in ['tbody', 'table', 'tr', 'td', 'th'])
    
    def _parse_table_mode(self, containers: List[Tag]) -> List[Dict]:
        """表格模式解析（標題、連結、日期、NID）"""
        data_list = []
        
        for i, row in enumerate(containers):
            try:
                if row.name != 'tr':
                    continue
                
                table_data = self._extract_table_row(row)
                if table_data and any(table_data.values()):
                    data_list.append(table_data)
                    logger.debug(f"表格行 {i+1}: {table_data.get('title', '')[:50]}")
            except Exception as e:
                logger.error(f"解析表格行 {i+1} 失敗: {e}")
                continue
        
        return data_list
    
    def _parse_general_mode(self, containers: List[Tag]) -> List[Dict]:
        """一般模式解析"""
        sample_container = containers[0] if containers else None
        detected_fields = self._auto_detect_fields(sample_container) if sample_container else {}
        logger.info(f"自動偵測欄位: {list(detected_fields.keys())}")
        
        data_list = []
        for i, container in enumerate(containers):
            try:
                row = self._extract_general_row(container, detected_fields)
                if row and any(row.values()):
                    data_list.append(row)
            except Exception as e:
                logger.error(f"解析一般模式行 {i+1} 失敗: {e}")
                continue
        
        return data_list
    
    def _auto_detect_fields(self, sample_container: Tag) -> Dict[str, str]:
        """自動偵測欄位"""
        detected = {}
        
        for field_name, patterns in self.field_patterns.items():
            selectors, validators = patterns[:-1], [patterns[-1]]
            
            for pattern in selectors:
                if isinstance(pattern, tuple):
                    for selector in pattern:
                        elements = sample_container.select(selector)
                        for elem in elements:
                            if self._validate_element(elem, validators):
                                detected[field_name] = selector
                                logger.debug(f"偵測到 {field_name}: {selector}")
                                break
                        if field_name in detected:
                            break
                elif isinstance(pattern, str):
                    elements = sample_container.select(pattern)
                    for elem in elements:
                        if self._validate_element(elem, validators):
                            detected[field_name] = pattern
                            break
        
        if not detected:
            detected['title'] = '*'
            logger.info("未偵測到欄位，使用標題回退")
        
        return detected
    
    def _validate_element(self, element: Tag, validators: List) -> bool:
        """驗證元素"""
        text = element.get_text(strip=True)
        if not text or len(text) < 2:
            return False
        
        for validator in validators:
            if callable(validator):
                try:
                    if not validator(element):
                        return False
                except:
                    return False
        return True
    
    def _extract_table_row(self, row: Tag) -> Optional[Dict]:
        """提取表格行資料（標題、連結、日期、NID）"""
        cells = row.find_all(['td', 'th'])
        if not cells:
            return None
        
        row_data = {}
        
        # 第一欄：標題 + 連結
        if len(cells) > 0:
            title_cell = cells[0]
            title_link = title_cell.find('a')
            if title_link:
                row_data['title'] = title_link.get_text(strip=True)
                row_data['link'] = title_link.get('href', '')
                nid_match = re.search(r'nid=([A-F0-9]+)', row_data['link'])
                if nid_match:
                    row_data['nid'] = nid_match.group(1)
            
            # 日期（從第一欄的隱藏 span）
            date_span = title_cell.find('span', class_=re.compile(r'hidden'))
            if date_span:
                date_text = date_span.get_text(strip=True).replace(',', '')
                date_match = re.search(r'\d{4}-\d{2}-\d{2}', date_text)
                if date_match:
                    row_data['date'] = date_match.group(0)
        
        # 第二欄：主要日期
        if len(cells) > 1:
            date_cell = cells[1].get_text(strip=True).strip()
            if date_cell and re.match(r'\d{4}-\d{2}-\d{2}', date_cell):
                row_data['date'] = date_cell
        
        return row_data if row_data.get('title') else None
    
    def _extract_general_row(self, container: Tag, detected_fields: Dict[str, str]) -> Optional[Dict]:
        """一般模式提取（標題、日期、連結）"""
        row = {}
        
        for field_name, selector in detected_fields.items():
            try:
                if selector == '*':
                    row[field_name] = container.get_text(strip=True)[:100]
                else:
                    elements = container.select(selector)
                    if elements:
                        element = elements[0]
                        if field_name == 'link':
                            href = element.get('href', '')
                            row[field_name] = href
                            id_match = re.search(r'id|nid=([A-F0-9]+)', href)
                            if id_match:
                                row['nid'] = id_match.group(1)
                        elif field_name == 'date':
                            date_text = element.get_text(strip=True).strip()
                            date_match = re.search(r'\d{4}-\d{2}-\d{2}', date_text)
                            if date_match:
                                row[field_name] = date_match.group(0)
                        else:
                            row[field_name] = element.get_text(strip=True)
            except Exception:
                continue
        
        if not any(row.values()):
            content = container.get_text(strip=True)
            if content:
                row['title'] = content[:100]
        
        return row if row.get('title') else None
    
    def validate_data(self, data_list: List[Dict]) -> List[Dict]:
        """資料驗證與清理"""
        valid_data = []
        for item in data_list:
            cleaned = {k: v for k, v in item.items() if v}
            if cleaned.get('title'):
                valid_data.append(item)
        return valid_data
    
    def get_stats(self, data_list: List[Dict]) -> Dict:
        """解析統計"""
        if not data_list:
            return {"total": 0, "fields_detected": {}}
        
        field_counts = {}
        for item in data_list:
            for key, value in item.items():
                if value:
                    field_counts[key] = field_counts.get(key, 0) + 1
        
        return {
            "total": len(data_list),
            "fields_detected": field_counts,
            "unique_items": len(set((item.get('title', ''), item.get('date', '')) for item in data_list)),
            "avg_fields_per_item": sum(field_counts.values()) / max(1, len(data_list))
        }

    def detect_pagination(self, soup: BeautifulSoup, common_selectors: List[str] = None) -> Dict:
        """自動偵測分頁資訊"""
        if common_selectors is None:
            common_selectors = [
                '.pagination', '.pager', '[class*="page"]', '[class*="pagination"]',
                'ul.pagination', '.page-numbers', '#pagination', 'nav.pagination',
                '.paging', 'div.pager', '.pagination-info', '[id*="page"]',
                '.pager li', 'ul.page', '.pages', '[class*="nav"]'
            ]
        
        pagination_info = {
            'total_pages': 1,
            'current_page': 1,
            'page_pattern': 'pg={page}',
            'base_url': None,
            'pagination_selector': None
        }
        
        logger.info("開始偵測分頁資訊...")
        
        # 尋找分頁容器
        pagination_container = None
        for selector in common_selectors:
            pagination = soup.select_one(selector)
            if pagination:
                logger.info(f"找到分頁容器: {selector}")
                pagination_info['pagination_selector'] = selector
                pagination_container = pagination
                break
        
        # 偵測總頁數
        total_pages = self._extract_total_pages_from_pagination(soup, pagination_container)
        if total_pages > 1:
            pagination_info['total_pages'] = total_pages
            logger.info(f"偵測到總頁數: {total_pages}")
        
        # 偵測當前頁面
        current_page = self._extract_current_page(soup)
        if current_page:
            pagination_info['current_page'] = current_page
        
        # 設置URL模式
        base_url = self._get_base_url(soup)
        if base_url:
            pagination_info['base_url'] = base_url
            pagination_info['page_pattern'] = 'pg={page}'
        
        logger.info(f"分頁資訊: {pagination_info}")
        return pagination_info
    
    def _extract_total_pages_from_pagination(self, soup: BeautifulSoup, pagination: Optional[Tag]) -> int:
        """提取總頁數"""
        max_page = 1
        
        # 從連結提取
        links = soup.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            page_matches = re.findall(r'(?:pg=|page=)(\d+)', href)
            for page_num in page_matches:
                max_page = max(max_page, int(page_num))
        
        # 從分頁容器文字提取
        if pagination:
            text = pagination.get_text()
            matches = re.findall(r'共?(\d+)\s*頁|(\d+)\s*/\s*(\d+)\s*頁|total\s+(\d+)', text)
            for match in matches:
                for num in match:
                    if num:
                        max_page = max(max_page, int(num))
        
        # 從最後頁連結提取
        last_links = soup.find_all('a', string=re.compile(r'最後|last|尾頁|more|\d+$'))
        for link in last_links:
            page_text = link.get_text().strip()
            if page_text.isdigit():
                max_page = max(max_page, int(page_text))
        
        # 檢查是否有下一頁
        next_page = soup.find('a', string=re.compile(r'下一頁|next'))
        if next_page and max_page == 1:
            max_page = 2
        
        return max(1, max_page)
    
    def _extract_current_page(self, soup: BeautifulSoup) -> Optional[int]:
        """提取當前頁面"""
        current_url_elem = soup.find('meta', {'property': 'og:url'}) or soup.find('link', {'rel': 'canonical'})
        current_url = (current_url_elem.get('content') if current_url_elem else '')
        
        page_match = re.search(r'(?:pg=|page=)(\d+)', current_url)
        if page_match:
            return int(page_match.group(1))
        
        current_indicators = ['.active', '.current', '[aria-current="page"]', '.on']
        for selector in current_indicators:
            current_elem = soup.select_one(selector)
            if current_elem:
                page_text = current_elem.get_text().strip()
                if page_text.isdigit():
                    return int(page_text)
        
        return 1
    
    def _get_base_url(self, soup: BeautifulSoup) -> str:
        """獲取基礎URL"""
        canonical = soup.find('link', {'rel': 'canonical'})
        if canonical and canonical.get('href'):
            return canonical['href']
        
        og_url = soup.find('meta', {'property': 'og:url'})
        if og_url and og_url.get('content'):
            return og_url['content']
        
        links = soup.find_all('a', href=True)
        if links:
            return links[0]['href']
        
        return ""
    
    def _detect_page_pattern(self, base_url: str) -> str:
        """偵測分頁URL模式"""
        if not base_url:
            return "pg={page}"
        
        patterns = [
            (r'(pg=|page=)(\d+)', r'\1{page}'),
            (r'/(\d+)\.html', '/{page}.html'),
            (r'\?p=(\d+)', '?p={page}'),
            (r'&pg=(\d+)', '&pg={page}')
        ]
        
        for pattern, replacement in patterns:
            if re.search(pattern, base_url):
                return replacement
        
        return "pg={page}"

if __name__ == "__main__":
    pass