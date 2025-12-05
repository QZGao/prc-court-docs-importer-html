# 中國裁判文書匯入工具（HTML 版）

[English README](README.md)

將中國裁判文書從 HTML/JSONL 格式轉換為維基文庫相容的維基文本的 Python 工具。

## 概述

本工具處理中國裁判文書網文書資料，將其從原始 HTML 格式轉換為適合上傳至中文維基文庫（zhwikisource）的維基文本格式。

## 功能特色

- **串流處理**：高效處理大型 JSONL 檔案，無需將所有資料載入記憶體
- **HTML 解析**：從 HTML 內容中擷取文件結構，支援各種格式樣式
- **維基文本生成**：產生格式正確的維基文本，包含模板（`{{header}}`、`{{署名}}`、`{{印}}` 等）
- **中日韓文字清理**：移除 OCR 產生的錯誤，如中文字元間的多餘空格
- **署名格式化**：依照維基文庫慣例，正確處理法官／書記員職稱與姓名的間距
- **篩選功能**：僅處理特定文書類型（判決書、裁定書等）
- **可續傳**：支援檢查點，可中斷並恢復大型批次作業
- **地點推斷**：自動從法院名稱判斷文書地點

## 安裝

```bash
# 複製儲存庫
git clone https://github.com/QZGao/prc-court-docs-importer-html.git
cd prc-court-docs-importer-html

# 建立虛擬環境
python -m venv .venv
.venv\Scripts\activate  # Windows
# 或：source .venv/bin/activate  # Linux/Mac

# 安裝相依套件
pip install -r requirements.txt
```

## 第一階段：轉換

### 使用方式

#### 基本轉換

```bash
# 轉換 JSONL 檔案中的所有文書
python -m convert input.jsonl

# 將產生以下輸出檔案：
# - input_converted.jsonl  （成功轉換）
# - input_failed.jsonl     （失敗轉換，含錯誤詳情）
# - input_checkpoint.json  （進度檢查點）
```

#### 依文書類型篩選

```bash
# 僅轉換判決書
python -m convert input.jsonl --filter "*判决书"

# 轉換多種類型（以分號分隔）
python -m convert input.jsonl --filter "*判决书;*事令"
```

#### 限制輸出數量

```bash
# 僅轉換前 100 份符合條件的文書
python -m convert input.jsonl --filter "*判决书" --limit 100
```

#### 自訂輸出路徑

```bash
python -m convert input.jsonl \
    --output converted.jsonl \
    --errors failed.jsonl \
    --checkpoint progress.json
```

#### 人類可讀輸出

```bash
# 產生格式化的 TXT 檔案以供審閱
python -m convert input.jsonl --filter "*判决书" --limit 50 --txt output.txt
```

#### 儲存原始 JSON

```bash
# 儲存每份處理文書的原始 JSON（便於除錯）
python -m convert input.jsonl --filter "*判决书" --limit 50 --original originals.jsonl
```

#### 恢復中斷的作業

```bash
# 若被中斷（Ctrl+C），從檢查點恢復
python -m convert --resume input_checkpoint.json

# 恢復時也可設定新的限制數量
python -m convert --resume input_checkpoint.json --limit 200
```

### CLI 選項

| 選項 | 簡寫 | 說明 |
|------|------|------|
| `input` | | 輸入 JSONL 檔案路徑 |
| `--output` | `-o` | 輸出 JSONL 檔案路徑 |
| `--errors` | `-e` | 錯誤記錄 JSONL 檔案路徑 |
| `--filter` | `-f` | 依標題篩選文書的 Glob 模式 |
| `--limit` | `-l` | 最大成功轉換數量 |
| `--resume` | `-r` | 從檢查點檔案恢復 |
| `--checkpoint` | `-c` | 檢查點檔案路徑 |
| `--txt` | `-t` | 人類可讀 TXT 輸出路徑 |
| `--original` | | 儲存處理文書的原始 JSON |

### 輸入格式

本工具接受包含裁判文書記錄的 JSONL（JSON Lines）或格式化 JSON 檔案。每筆記錄應包含：

| 欄位 | 說明 |
|------|------|
| `s1` | 文書標題 |
| `s2` | 法院名稱 |
| `s7` | 案號（文書號） |
| `s22` | 完整層級字串（法院 + 文書類型 + 案號） |
| `qwContent` | 文書的 HTML 內容 |
| `wsKey` | 文書唯一識別碼 |

### 輸出格式

每行包含一個 JSON 物件：

```json
{
  "title": "文書標題",
  "wenshu_id": "唯一識別碼",
  "court": "法院名稱",
  "doc_type": "文書類型",
  "doc_id": "案號",
  "wikitext": "完整維基文本內容"
}
```

## 第二階段：上傳

上傳階段將已轉換的 JSONL 檔案上傳至 zhwikisource，具備速率限制與衝突解決功能。

### 設定

在專案根目錄建立 `.env` 檔案，填入您的機器人憑證：

```env
MW_BOT_USERNAME=您的使用者名稱
MW_BOT_PASSWORD=機器人名稱@您的機器人密碼
```

取得機器人憑證的步驟：

1. 登入 [zh.wikisource.org](https://zh.wikisource.org)
2. 前往 [Special:BotPasswords](https://zh.wikisource.org/wiki/Special:BotPasswords)
3. 建立新的機器人密碼，並授予適當權限（編輯、建立頁面、移動頁面）

### 上傳使用方式

#### 基本上傳

```bash
# 從已轉換的 JSONL 上傳所有文書
python -m upload converted.jsonl

# 將產生以下記錄檔：
# - uploaded_YYYYMMDD_HHMMSS.log      （成功上傳）
# - upload_failed_YYYYMMDD_HHMMSS.jsonl （失敗上傳）
# - skipped_YYYYMMDD_HHMMSS.log       （已跳過的頁面）
```

#### 限制上傳數量

```bash
# 僅上傳前 10 份文書（用於測試）
python -m upload converted.jsonl --max 10
```

#### 自訂速率限制

```bash
# 設定編輯間隔秒數（預設：10 秒，pywikibot 預設值）
python -m upload converted.jsonl --interval 5

# 設定 maxlag 參數（預設：5）
python -m upload converted.jsonl --maxlag 10
```

#### 停用衝突解決

```bash
# 跳過所有已存在的頁面（不嘗試解決）
python -m upload converted.jsonl --no-resolve
```

#### 自訂記錄目錄

```bash
# 將記錄儲存至指定目錄
python -m upload converted.jsonl --log-dir ./logs
```

### 上傳 CLI 選項

| 選項 | 簡寫 | 說明 |
|------|------|------|
| `input` | | 輸入已轉換的 JSONL 檔案路徑 |
| `--interval` | `-i` | 編輯間隔最小秒數（預設：10） |
| `--maxlag` | `-m` | API 的 maxlag 參數（預設：5） |
| `--max` | `-n` | 最大上傳文書數量 |
| `--no-resolve` | | 停用衝突解決 |
| `--log-dir` | | 記錄檔目錄 |

### 衝突解決

當已存在相同標題的頁面時，上傳工具可自動解決特定衝突：

1. **已存在 `{{versions}}` 頁面**：新增含案號專屬標題的條目
2. **已存在來自相同法院的 `{{header}}` 頁面**：
   - 將現有頁面移動至案號專屬標題
   - 在原標題建立 `{{versions}}` 頁面
   - 將新文書上傳至其案號專屬標題

無法自動解決的衝突將記錄至跳過記錄檔。

### 速率限制

速率限制由 pywikibot 內建的節流機制處理：

- **編輯間隔**：使用 pywikibot 的 `put_throttle` 設定（預設 10 秒，可透過 `--interval` 調整）
- **Maxlag 處理**：pywikibot 自動遵守伺服器端的 maxlag 回應
- **節流檔案**：pywikibot 會寫入 `throttle.ctrl` 以管理節流狀態

## 授權條款

MIT 授權條款 - 詳見 [LICENSE](LICENSE)。

部分程式碼採用自 [prc-court-docs-importer-csv](https://github.com/QZGao/prc-court-docs-importer-csv)。
