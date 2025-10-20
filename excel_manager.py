import pandas as pd
import os
from typing import List, Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class ExcelManager:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self._ensure_directory()
    
    def _ensure_directory(self):
        """確保資料夾存在"""
        directory = os.path.dirname(self.filepath)
        if directory:
            os.makedirs(directory, exist_ok=True)
    
    def load_data(self) -> List[Dict]:
        """載入現有Excel資料"""
        if not os.path.exists(self.filepath):
            logger.debug(f"Excel檔案不存在: {self.filepath}，將創建新檔案")
            return []
        
        try:
            df = pd.read_excel(self.filepath, engine='openpyxl')
            logger.info(f"載入 {len(df)} 筆現有資料")
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"讀取Excel失敗: {e}")
            return []
    
    def save_data(self, data_list: List[Dict], columns: List[str] = None) -> None:

        if not data_list:
            logger.warning("無資料可儲存")
            return
        
        try:
            df = pd.DataFrame(data_list)
            
            # 強制指定欄位順序
            if columns:
                df = df.reindex(columns=columns)
            
            # 如果檔案存在，合併資料並去重
            if os.path.exists(self.filepath):
                existing_data = self.load_data()
                if existing_data:
                    existing_df = pd.DataFrame(existing_data)
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                else:
                    combined_df = df
            else:
                combined_df = df
            
            # 使用活動名稱和起日去重，保留最新抓取的記錄
            combined_df.drop_duplicates(subset=['活動名稱', '起日'], keep='last', inplace=True)
            
            # --- 新增的排序步驟 ---
            # 1. 確保 '起日' 欄位是日期格式，以便正確排序。
            #    errors='coerce' 會讓無法轉換的無效日期變成空值(NaT)。
            combined_df['起日'] = pd.to_datetime(combined_df['起日'], errors='coerce')
            
            # 2. 根據 '起日' 進行排序， ascending=True 代表由舊到新 (升序)。
            #    na_position='last' 會將沒有日期的資料排在最下面。
            combined_df.sort_values(by='起日', ascending=True, inplace=True, na_position='last')
            
            # 3. (可選) 將日期格式轉回 'YYYY-MM-DD' 字串，讓 Excel 顯示更乾淨
            combined_df['起日'] = combined_df['起日'].dt.strftime('%Y-%m-%d')
            
            # 儲存到Excel
            with pd.ExcelWriter(self.filepath, engine='openpyxl') as writer:
                combined_df.to_excel(writer, sheet_name='監控資料', index=False)
            
            # 自動調整欄寬
            self._auto_adjust_columns()
            
            logger.info(f"資料已排序並儲存到: {self.filepath}")
            print(f"已儲存 {len(combined_df)} 筆資料到 {self.filepath}")
            
        except Exception as e:
            logger.error(f"儲存Excel失敗: {e}", exc_info=True)
    
    def _auto_adjust_columns(self):
        """自動調整Excel欄寬"""
        try:
            from openpyxl import load_workbook
            wb = load_workbook(self.filepath)
            ws = wb['監控資料']
            
            for column in ws.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if cell.value is not None:
                            max_length = max(max_length, len(str(cell.value)))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                ws.column_dimensions[column_letter].width = adjusted_width
            
            wb.save(self.filepath)
        except Exception as e:
            logger.warning(f"調整欄寬失敗: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """取得Excel統計資訊"""
        if not os.path.exists(self.filepath):
            return {"total_records": 0, "fields_detected": {}, "last_update": None}
        
        try:
            df = pd.read_excel(self.filepath, engine='openpyxl')
            fields_detected = {col: sum(df[col].notna()) for col in df.columns}
            return {
                "total_records": len(df),
                "fields_detected": fields_detected,
                "last_update": df['抓取時間'].max() if '抓取時間' in df.columns else None
            }
        except Exception:
            return {"total_records": 0, "fields_detected": {}, "last_update": None}