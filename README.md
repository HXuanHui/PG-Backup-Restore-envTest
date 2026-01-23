# PostgreSQL 備份還原自動化測試腳本

這個專案提供兩個版本的腳本，可以自動化執行 PostgreSQL 備份還原測試，並記錄 CPU 使用率和執行時間。

## 功能

- 透過 SSH 遠端連線到資料庫伺服器
- 自動執行不同資料大小的測試（1GB、3GB、5GB、9GB）
- 記錄每個步驟的執行時間和 CPU 使用率
- 生成詳細的測試報告（JSON 和文字格式）

## 測試流程

對每個資料大小執行以下步驟：

1. **資料填充**: 使用 `pgbench` 初始化測試資料
2. **記錄資料量大小**: 查詢實際資料庫大小
3. **備份**: 執行 `pgbackrest` 完整備份（記錄時間和 CPU）
4. **停止 PostgreSQL**: 停止資料庫服務
5. **刪除資料目錄**: 清空資料目錄以模擬災難恢復
6. **還原**: 執行 `pgbackrest` 還原（記錄時間和 CPU）

## 版本選擇

### Python 版本（推薦）

功能更完整，互動性更好，錯誤處理更完善。

**安裝依賴：**
```bash
pip install -r requirements.txt
```

**使用方法：**
```bash
python pg_backup_restore_test.py
```

執行後會提示輸入：
- SSH 主機名稱或 IP
- SSH 使用者名稱
- 認證方式（密碼或金鑰檔案）
- 對應的認證資訊

### Shell 腳本版本

更簡單，不需要 Python 依賴，但需要遠端伺服器安裝 `bc` 命令。

**使用方法：**
```bash
chmod +x pg_backup_restore_test.sh
./pg_backup_restore_test.sh <SSH_HOST> <SSH_USER> [SSH_KEY_FILE]
```

**範例：**
```bash
# 使用密碼認證
./pg_backup_restore_test.sh 192.168.1.100 admin

# 使用 SSH 金鑰認證
./pg_backup_restore_test.sh 192.168.1.100 admin ~/.ssh/id_rsa
```

**注意：** Shell 版本需要遠端伺服器安裝 `bc` 命令（用於計算）：
```bash
# Ubuntu/Debian
sudo apt-get install bc

# CentOS/RHEL
sudo yum install bc
```

## 輸出

腳本會生成兩個報告檔案：

1. **test_results.json**: 完整的測試結果（JSON 格式）
2. **test_results_report.txt**: 易讀的文字報告

## 注意事項

- 確保遠端伺服器已安裝並配置好：
  - PostgreSQL 16
  - pgbackrest
  - pgbench
  - 適當的 sudo 權限
- 確保 PostgreSQL 服務名稱為 `postgresql@16-test`
- 確保 pgbackrest stanza 名稱為 `test-backup`
- 確保 PostgreSQL 監聽在 5433 埠
- 測試過程可能需要較長時間，請耐心等待

## 自訂設定

如果需要修改測試的資料大小，可以編輯腳本中的 `test_configs` 變數：

```python
test_configs = [
    (68, "1GB"),   # scale factor, 目標大小（根據實際測試，每個 scale factor ≈ 15 MB）
    (205, "3GB"),
    (341, "5GB"),
    (614, "9GB")
]
```

pgbench 的 scale factor 約略對應（根據實際測試結果，每個 scale factor ≈ 15 MB）：
- `-s 68` ≈ 1GB (1024 MB)
- `-s 205` ≈ 3GB (3072 MB)
- `-s 341` ≈ 5GB (5120 MB)
- `-s 614` ≈ 9GB (9216 MB)

實際大小可能因資料內容而略有差異。
