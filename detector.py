from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DataDetector:
    def __init__(self):
        pass
    
    def detect_new_data(self, new_data: List[Dict], existing_data: List[Dict]) -> List[Dict]:
        """檢測新資料，使用活動名稱和日期去重"""
        if not new_data:
            logger.info("無新資料需要檢測")
            return []
        
        # 使用活動名稱和日期組合作為唯一識別
        existing_keys = {(item.get('活動名稱', ''), item.get('起日', '')) for item in existing_data 
                        if item.get('活動名稱') and item.get('起日')}
        logger.debug(f"現有活動名稱+日期組合數量: {len(existing_keys)}")
        
        new_items = [item for item in new_data 
                     if (item.get('title', ''), item.get('date', '')) not in existing_keys]
        
        logger.info(f"檢測到 {len(new_items)} 筆新資料")
        return new_items
    
    def has_new_data(self, new_data: List[Dict], existing_data: List[Dict]) -> bool:
        """快速檢查是否有新資料"""
        existing_hashes = {item['hash'] for item in existing_data}
        for item in new_data:
            if item['hash'] not in existing_hashes:
                return True
        return False
    
    def get_changes_summary(self, new_items: List[Dict]) -> Dict[str, Any]:
        """產生變更摘要"""
        if not new_items:
            return {"count": 0, "types": {}}
        
        type_count = {}
        for item in new_items:
            item_type = item.get('source', 'unknown')
            type_count[item_type] = type_count.get(item_type, 0) + 1
        
        return {
            "count": len(new_items),
            "types": type_count,
            "first_check_time": min(item['check_time'] for item in new_items),
            "latest_check_time": max(item['check_time'] for item in new_items)
        }