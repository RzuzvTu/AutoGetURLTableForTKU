#!/usr/bin/env python3
"""
411631236
網頁監控系統 - 淡江大學深耕網專用
功能：
- 單次抓取指定網站的表格資料
- 在列表頁使用活動名稱和日期去重
- 僅對新資料進入詳細頁提取時間、地點、主辦單位
- 支援 spid/pg 分頁切換
- 特定網站過濾：僅保留主辦單位為「教師教學發展中心」或「教發中心」的資料
- 輸出到 Excel（活動名稱、主辦單位、地點、起日、迄日、抓取時間）
"""

import os
import sys
import json
import sys
import hashlib
import re
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List
import traceback
import logging
import concurrent.futures 
from dateutil.parser import parse as date_parse
from dateutil.parser import ParserError
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

def get_resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('data/monitor.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# 導入模組
try:
    from smart_parser import SmartParser
    from crawler import WebCrawler
    from detector import DataDetector
    from excel_manager import ExcelManager
    from paginator import Paginator
except ImportError as e:
    logger.error(f"模組導入失敗: {e}")
    logger.error("請確保以下檔案存在：")
    logger.error("  - smart_parser.py")
    logger.error("  - crawler.py")
    logger.error("  - detector.py")
    logger.error("  - excel_manager.py")
    logger.error("  - paginator.py")
    sys.exit(1)

def load_simple_config() -> Optional[Dict[Any, Any]]:
    """載入配置檔案"""
    try:
        config_path_str = get_resource_path('config.json') 
        config_path = Path(config_path_str)
        if not config_path.exists():
            logger.error("找不到 config.json 檔案")

            return None
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # 驗證必要欄位
        sites = config.get('sites', [])
        if not sites:
            logger.error("config.json 中沒有 'sites' 配置")
            return None
            
        for site in sites:
            required_fields = ['name', 'url', 'container_selector', 'excel_file']
            missing = [field for field in required_fields if field not in site]
            if missing:
                raise ValueError(f"網站 '{site.get('name', '未知')}' 缺少欄位: {missing}")
        
        logger.info(f"成功載入 {len(sites)} 個監控目標")
        for site in sites:
            status = "on" if site.get('enabled', True) else "off"
            logger.info(f"{status} {site['name']}: {site['container_selector']}")
        
        return config
        
    except json.JSONDecodeError as e:
        logger.error(f"config.json 格式錯誤 (第 {e.lineno} 行): {e}")
        return None
    except Exception as e:
        logger.error(f"配置載入失敗: {e}")
        return None

def create_default_headers(config: Optional[Dict[Any, Any]] = None) -> Dict[str, str]:
    """建立HTTP標頭，優先使用 config.json 中的設定"""

    default_user_agent = 'TKU-Sprout-Monitor/2.2 (+https://github.com/your-repo/or-contact-email)'
    
    if config and 'headers' in config and 'User-Agent' in config['headers']:
        user_agent = config['headers']['User-Agent']
    else:
        user_agent = default_user_agent
        logger.info(f"未在 config.json 中找到 User-Agent，使用預設值: {user_agent}")

    return {
        'User-Agent': user_agent,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    }

def fetch_detail_page(crawler: WebCrawler, base_url: str, item: Dict, max_retries: int = 3) -> Optional[Dict]:
    """抓取詳細頁面資料（時間、地點、主辦單位），使用更穩健的日期解析"""
    detail_url = item.get('link', '')
    if not detail_url:
        logger.debug(f"項目 '{item.get('title', '無標題')}' 無詳細頁面連結")
        return None

    # 補全相對URL
    detail_url = urljoin(base_url, detail_url)
    logger.info(f"抓取詳細頁: {detail_url}")

    # 重試邏輯
    for attempt in range(max_retries):
        try:
            soup = crawler.fetch(detail_url)
            if not soup:
                logger.warning(f"詳細頁抓取失敗: {detail_url} (嘗試 {attempt+1}/{max_retries})")
                time.sleep(2)
                continue

            # 解析詳細頁
            detail_data = {
                '活動名稱': item.get('title', '').strip(),
                '抓取時間': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                '主辦單位': None,
                '地點': None,
                '起日': item.get('date', None),
                '迄日': item.get('date', None)
            }

            # 嚴格限定在 div.clsDtlDes
            detail_container = soup.select_one('div.clsDtlDes, [class*="DtlDes"]')
            if not detail_container:
                logger.warning(f"未找到詳細頁容器: {detail_url}，將嘗試解析整個頁面")
                detail_container = soup

            # 時間 (使用 dateutil 增強穩健性)
            time_elem = detail_container.find('p', string=re.compile(r'^時\s*間\s*：', re.IGNORECASE))
            if not time_elem:
                time_elem = soup.select_one('p#DtlDt')
            if time_elem:
                time_text = time_elem.get_text(strip=True).replace('時間：', '').replace('Time:', '').strip()
                logger.debug(f"時間原始文字: {time_text}")
                try:
                    # 優先處理日期範圍，支援 `~` `-` `至`
                    range_match = re.search(r'(\d{4}[/-]\d{1,2}[/-]\d{1,2})\s*[-~至]\s*(\d{4}[/-]\d{1,2}[/-]\d{1,2})', time_text, re.IGNORECASE)
                    if range_match:
                        start_date = date_parse(range_match.group(1)).strftime('%Y-%m-%d')
                        end_date = date_parse(range_match.group(2)).strftime('%Y-%m-%d')
                        detail_data['起日'] = start_date
                        detail_data['迄日'] = end_date
                    else:
                        # 處理單一日期，fuzzy=True 會忽略周圍不相關的文字
                        parsed_date = date_parse(time_text, fuzzy=True)
                        date_str = parsed_date.strftime('%Y-%m-%d')
                        detail_data['起日'] = date_str
                        detail_data['迄日'] = date_str
                    logger.debug(f"提取時間成功: 起日={detail_data['起日']}, 迄日={detail_data['迄日']}")
                except (ParserError, TypeError):
                    logger.warning(f"使用 dateutil 無法從 '{time_text}' 提取日期")

            # 地點
            location_elem = detail_container.find('p', string=re.compile(r'^地\s*點\s*：', re.IGNORECASE))
            if location_elem:
                location_text = location_elem.get_text(strip=True)
                detail_data['地點'] = re.sub(r'地\s*點\s*：?|Location\s*:|Venue\s*:', '', location_text, flags=re.IGNORECASE).strip()
                logger.debug(f"提取地點: {detail_data['地點']}")

            # 主辦單位
            organizer_elem = detail_container.find('p', string=re.compile(r'^主辦單位\s*：', re.IGNORECASE))
            if organizer_elem:
                organizer_text = organizer_elem.get_text(strip=True)
                detail_data['主辦單位'] = re.sub(r'主辦單位\s*：?|Organizer\s*:', '', organizer_text, flags=re.IGNORECASE).strip()
                logger.debug(f"提取主辦單位: {detail_data['主辦單位']}")

            if not detail_data['活動名稱']:
                logger.warning(f"無效項目 (缺少活動名稱): {detail_url}")
                return None

            return detail_data

        except Exception as e:
            logger.error(f"詳細頁解析失敗 ({detail_url}) 嘗試 {attempt+1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(3) # 增加失敗時的等待時間
            continue

    logger.error(f"詳細頁抓取徹底失敗 ({detail_url})，已達最大重試次數")
    return None # 直接返回 None，避免儲存不完整資料

def filter_data_by_organizer(data: List[Dict], site_config: Dict[str, Any]) -> List[Dict]:
    """根據主辦單位過濾資料"""
    target_url = "https://sprout.tku.edu.tw/DeepNews.aspx?spid=76E2C6DA-A2BA-410D-8BA0-CBFB22C84AEA"
    if site_config.get('url') != target_url:
        logger.debug(f"網站 {site_config['name']} 不需過濾主辦單位")
        return data
    
    filtered_data = [
        item for item in data 
        if item and item.get('主辦單位', '') in ['教師教學發展中心', '教發中心']
    ]
    logger.info(f"過濾後保留 {len(filtered_data)} 筆資料（主辦單位：教師教學發展中心或教發中心）")
    return filtered_data


def monitor_site(site_config: Dict[str, Any], crawler: WebCrawler) -> List[Dict]:
    """監控單一網站，包含 robots.txt 檢查和路徑安全驗證"""
    if not site_config.get('enabled', True):
        logger.info(f"跳過停用的網站: {site_config['name']}")
        return []

    logger.info("\n" + "="*70)
    logger.info(f"開始檢查: {site_config['name']} ({site_config['url']})")
    logger.info("="*70)

    try:
        rp = RobotFileParser()
        robots_url = urljoin(site_config['url'], 'robots.txt')
        rp.set_url(robots_url)
        rp.read()
        if not rp.can_fetch(crawler.headers.get('User-Agent'), site_config['url']):
            logger.warning(f"網站 '{site_config['name']}' 的 robots.txt 禁止抓取此 URL。已跳過。")
            return []
        logger.info("robots.txt 檢查通過，允許抓取。")
    except Exception as e:
        logger.warning(f"檢查 robots.txt 失敗: {e}，將繼續抓取。")

    try:
        excel_file_path = Path(site_config['excel_file'])
        data_dir = Path("data").resolve()
        
        resolved_path = data_dir.joinpath(excel_file_path.name).resolve()
        # 確保路徑在 data 目錄下
        if not resolved_path.is_relative_to(data_dir):
            logger.error(f"不安全的檔案路徑: '{site_config['excel_file']}'。路徑必須在 '{data_dir}' 資料夾內。已跳過。")
            return []
        excel_mgr = ExcelManager(str(resolved_path))
    except Exception as e:
        logger.error(f"檔案路徑處理失敗: {e}", exc_info=True)
        return []

    parser = SmartParser(max_items=site_config.get('max_items', 100))
    detector = DataDetector()
    table_mode = site_config.get('table_mode', False)
    use_pagination = site_config.get('auto_pagination', True)
    
    try:
        if use_pagination and Paginator:
            logger.info("啟用自動分頁抓取...")
            paginator = Paginator(crawler, parser)
            list_data = paginator.auto_fetch_all_pages(
                site_config['url'],
                site_config['container_selector'],
                max_pages=site_config.get('max_pages', 10),
                table_mode=table_mode,
                pagination_config=site_config.get('pagination', {})
            )
        else:
            logger.info("單頁抓取模式...")
            soup = crawler.fetch(site_config['url'])
            if not soup:
                logger.error("抓取失敗")
                return []
            list_data = parser.parse(
                soup, 
                site_config['container_selector'],
                table_mode=table_mode
            )
        
        if not list_data:
            logger.info("在列表頁未找到任何資料")
            return []
        
        logger.info(f"從列表頁解析到 {len(list_data)} 筆資料")
        
        # 比對新舊資料（使用活動名稱和日期）
        logger.info("比對新舊資料...")
        existing_data = excel_mgr.load_data()
        new_items = detector.detect_new_data(list_data, existing_data)
        
        if not new_items:
            logger.info("沒有發現新資料")
            return []
        
        logger.info(f"發現 {len(new_items)} 筆新資料，開始抓取詳細頁面...")
        
        # 僅對新資料抓取詳細頁
        detailed_data = []
        for i, item in enumerate(new_items, 1):
            logger.debug(f"處理新項目 {i}/{len(new_items)}: {item.get('title', '無標題')[:50]}")
            detail = fetch_detail_page(crawler, site_config['url'], item)
            if detail:
                detailed_data.append(detail)
        
        logger.info(f"成功抓取 {len(detailed_data)} 筆詳細資料")
        
        # 過濾主辦單位
        filtered_data = filter_data_by_organizer(detailed_data, site_config)
        
        if not filtered_data:
            logger.info("經過主辦單位過濾後，無符合條件的新資料")
            return []
        
        logger.info(f"過濾後保留 {len(filtered_data)} 筆資料，準備儲存...")
        
        # 儲存
        excel_mgr.save_data(filtered_data, columns=['活動名稱', '主辦單位', '地點', '起日', '迄日', '抓取時間'])
        
        # 統計
        stats = excel_mgr.get_stats()
        logger.info(f"儲存完成。Excel 總計: {stats.get('total_records', 0)} 筆記錄")
        
        return filtered_data
        
    except Exception as e:
        logger.error(f"網站 '{site_config['name']}' 監控過程中發生未預期錯誤", exc_info=True)
        return []

def main() -> None:

    logger.info("系統啟動")
    logger.info("=" * 70)
    
    # 確保資料目錄
    data_dir = Path("data") 
    data_dir.mkdir(exist_ok=True)
    logger.info(f"資料目錄: {data_dir.absolute()}")
    
    # 載入配置
    config = load_simple_config()
    if not config:
        logger.critical("配置載入失敗，程式結束。請檢查 config.json 檔案。")
        input("按 Enter 鍵退出...")
        return
    
    # 初始化爬蟲（共用session）
    headers = create_default_headers(config) 
    crawler = WebCrawler(headers)
    
    # 執行所有網站抓取
    all_data = []
    enabled_sites = [site for site in config.get('sites', []) if site.get('enabled', True)]
    
    if not enabled_sites:
        logger.warning("沒有任何啟用的監控網站，程式結束。")
        input("按 Enter 鍵退出...")
        return

    logger.info(f"準備開始並行抓取 {len(enabled_sites)} 個啟用的網站...")
    
    max_workers = config.get('settings', {}).get('max_concurrent_sites', 5)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

        future_to_site = {executor.submit(monitor_site, site, crawler): site for site in enabled_sites}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_site), 1):
            site = future_to_site[future]
            logger.info(f"--- 進度 {i}/{len(enabled_sites)} ---")
            try:
                site_data = future.result()
                if site_data:
                    all_data.extend(site_data)
                logger.info(f"網站 '{site['name']}' 處理完成")
            except Exception:
                logger.error(f"網站 '{site['name']}' 在執行過程中發生嚴重錯誤", exc_info=True)

    logger.info("\n" + "="*70)
    logger.info(f"所有網站抓取完成！總計處理 {len(all_data)} 筆新資料")
    logger.info(f"所有資料已儲存至: {data_dir.absolute()}")
    logger.info("="*70)
    
    crawler.close()
    input("按 Enter 鍵退出...")

if __name__ == "__main__":
    main()