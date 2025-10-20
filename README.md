# 淡江大學深耕計畫網頁監控爬蟲

這是一個 Python 爬蟲專案，為了自動化我部分簡單的工作而設計，主要用於淡江大學高教深耕網抓取特定資料。

## 主要功能

- **設定檔驅動**：可透過 `config.json` 輕鬆新增、修改或停用監控目標。
- **智慧解析**：自動解析列表頁和詳細頁的活動資訊（如主辦單位、地點、日期）。
- **增量更新**：只處理新發現的活動，避免重複抓取和儲存。
- **自動分頁**：能自動偵測並抓取多個分頁的資料。
- **安全設計**：內建檔案路徑驗證，防止惡意設定。

## 如何開始

### 先決條件

- Python 3.13 或更高版本

### 安裝步驟

1.  **建立並啟動虛擬環境**
    ```bash
    python -m venv venv
    # Windows
    .\venv\Scripts\activate
    # macOS / Linux
    source venv/bin/activate
    ```

2.  **安裝依賴套件**
    ```bash
    pip install -r requirements.txt
    ```

## 使用說明

1.  **設定 `config.json`**
    - 複製 `config.example.json` 並重新命名為 `config.json`。
    - 根據您的需求修改 `config.json` 中的網站 `url`、`container_selector` 和 `excel_file` 等欄位。
    - **重要**：請在 `headers` 的 `User-Agent` 中提供您的聯絡方式（GitHub 或 Email）。

2.  **執行爬蟲**
    ```bash
    python main.py
    ```
    執行完畢後，最新的資料將會被儲存到您在 `config.json` 中指定的 Excel 檔案（位於 `data/` 資料夾下）。
