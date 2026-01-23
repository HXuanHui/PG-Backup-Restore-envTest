#!/bin/bash
# PostgreSQL 備份還原自動化測試腳本 (Shell 版本)
# 使用方法: ./pg_backup_restore_test.sh [SSH_HOST] [SSH_USER] [SSH_KEY_FILE]

set -e

# 顏色定義
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# SSH 連線參數
SSH_HOST="${1:-}"
SSH_USER="${2:-}"
SSH_KEY="${3:-}"
SSH_OPTS=""

if [ -z "$SSH_HOST" ] || [ -z "$SSH_USER" ]; then
    echo "使用方法: $0 <SSH_HOST> <SSH_USER> [SSH_KEY_FILE]"
    echo "範例: $0 192.168.1.100 admin ~/.ssh/id_rsa"
    exit 1
fi

if [ -n "$SSH_KEY" ]; then
    SSH_OPTS="-i $SSH_KEY"
fi

# 測試配置 (scale_factor, 目標大小)
declare -a TEST_CONFIGS=(
    "10:1GB"
    "30:3GB"
    "50:5GB"
    "90:9GB"
)

# 結果檔案
RESULT_FILE="test_results_$(date +%Y%m%d_%H%M%S).txt"
JSON_FILE="test_results_$(date +%Y%m%d_%H%M%S).json"

echo "=========================================="
echo "PostgreSQL 備份還原自動化測試"
echo "=========================================="
echo "主機: $SSH_HOST"
echo "使用者: $SSH_USER"
echo "結果檔案: $RESULT_FILE"
echo "=========================================="
echo ""

# 初始化 JSON 檔案
echo "[" > "$JSON_FILE"

# 執行遠端命令並記錄時間
execute_remote() {
    local cmd="$1"
    local description="$2"
    
    echo -e "${YELLOW}[執行]${NC} $description"
    
    # 執行命令並記錄時間
    local start_time=$(date +%s.%N)
    local output=$(ssh $SSH_OPTS "$SSH_USER@$SSH_HOST" "$cmd" 2>&1)
    local exit_code=$?
    local end_time=$(date +%s.%N)
    
    # 計算執行時間
    local elapsed=$(echo "$end_time - $start_time" | bc)
    local elapsed_min=$(echo "scale=0; $elapsed / 60" | bc)
    local elapsed_sec=$(echo "scale=2; $elapsed - ($elapsed_min * 60)" | bc)
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}[完成]${NC} $description - 耗時: ${elapsed_min}分${elapsed_sec}秒"
    else
        echo -e "${RED}[失敗]${NC} $description - 退出碼: $exit_code"
        echo "$output" | head -5
    fi
    
    echo "$output"
    return $exit_code
}

# 執行命令並監控 CPU
execute_with_cpu_monitor() {
    local cmd="$1"
    local description="$2"
    
    echo -e "${YELLOW}[執行]${NC} $description"
    
    # 啟動 CPU 監控
    local monitor_pid=$(ssh $SSH_OPTS "$SSH_USER@$SSH_HOST" "
        (
            while true; do
                if command -v vmstat >/dev/null 2>&1; then
                    vmstat 1 1 | tail -1 | awk '{print 100-\$15}'
                else
                    top -bn1 | grep 'Cpu(s)' | awk '{print \$2}' | sed 's/%us,//'
                fi
                sleep 1
            done
        ) > /tmp/cpu_monitor_\$\$.log 2>&1 &
        echo \$!
        echo /tmp/cpu_monitor_\$\$.log
    " | head -2)
    
    local pid=$(echo "$monitor_pid" | head -1)
    local log_file=$(echo "$monitor_pid" | tail -1)
    
    # 執行主要命令
    local start_time=$(date +%s.%N)
    local output=$(ssh $SSH_OPTS "$SSH_USER@$SSH_HOST" "$cmd" 2>&1)
    local exit_code=$?
    local end_time=$(date +%s.%N)
    
    # 停止監控並取得平均 CPU
    local avg_cpu=$(ssh $SSH_OPTS "$SSH_USER@$SSH_HOST" "
        kill $pid 2>/dev/null; wait $pid 2>/dev/null 2>&1
        cat $log_file 2>/dev/null | awk '{sum+=\$1; count++} END {if(count>0) print sum/count; else print 0}'
        rm -f $log_file 2>/dev/null
    ")
    
    # 計算執行時間
    local elapsed=$(echo "$end_time - $start_time" | bc)
    local elapsed_min=$(echo "scale=0; $elapsed / 60" | bc)
    local elapsed_sec=$(echo "scale=2; $elapsed - ($elapsed_min * 60)" | bc)
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}[完成]${NC} $description"
        echo "  耗時: ${elapsed_min}分${elapsed_sec}秒"
        echo "  平均 CPU: ${avg_cpu}%"
    else
        echo -e "${RED}[失敗]${NC} $description - 退出碼: $exit_code"
    fi
    
    echo "ELAPSED:$elapsed"
    echo "CPU:$avg_cpu"
    return $exit_code
}

# 執行單一測試
run_test() {
    local scale_factor="$1"
    local target_size="$2"
    local is_first="$3"
    
    echo ""
    echo "=========================================="
    echo "測試: $target_size (scale factor: $scale_factor)"
    echo "=========================================="
    
    # 步驟 1: 資料填充
    echo ""
    echo "[步驟 1/6] 資料填充..."
    local fill_result=$(execute_with_cpu_monitor \
        "sudo -u postgres pgbench -i -s $scale_factor -p 5433 postgres" \
        "資料填充 ($target_size)")
    local fill_time=$(echo "$fill_result" | grep "^ELAPSED:" | cut -d: -f2)
    local fill_cpu=$(echo "$fill_result" | grep "^CPU:" | cut -d: -f2)
    
    sleep 2
    
    # 步驟 2: 記錄資料量大小
    echo ""
    echo "[步驟 2/6] 記錄資料量大小..."
    local db_size=$(execute_remote \
        "sudo -u postgres psql -p 5433 -c \"SELECT pg_size_pretty(pg_database_size('postgres'));\"" \
        "查詢資料庫大小")
    db_size=$(echo "$db_size" | grep -oE '[0-9]+\.[0-9]+\s+[A-Za-z]+|[0-9]+\s+[A-Za-z]+' | head -1)
    
    sleep 2
    
    # 步驟 3: 備份
    echo ""
    echo "[步驟 3/6] 執行備份..."
    local backup_result=$(execute_with_cpu_monitor \
        "sudo -u postgres pgbackrest --stanza=test-backup --type=full backup" \
        "備份 ($target_size)")
    local backup_time=$(echo "$backup_result" | grep "^ELAPSED:" | cut -d: -f2)
    local backup_cpu=$(echo "$backup_result" | grep "^CPU:" | cut -d: -f2)
    
    sleep 2
    
    # 步驟 4: 停止 PostgreSQL
    echo ""
    echo "[步驟 4/6] 停止 PostgreSQL..."
    execute_remote "sudo systemctl stop postgresql@16-test" "停止 PostgreSQL"
    
    sleep 2
    
    # 步驟 5: 刪除資料目錄
    echo ""
    echo "[步驟 5/6] 刪除資料目錄..."
    execute_remote "sudo bash -c 'rm -rf /var/lib/postgresql/16/test/*'" "刪除資料目錄"
    
    sleep 2
    
    # 步驟 6: 還原
    echo ""
    echo "[步驟 6/6] 執行還原..."
    local restore_result=$(execute_with_cpu_monitor \
        "sudo -u postgres pgbackrest --stanza=test-backup restore" \
        "還原 ($target_size)")
    local restore_time=$(echo "$restore_result" | grep "^ELAPSED:" | cut -d: -f2)
    local restore_cpu=$(echo "$restore_result" | grep "^CPU:" | cut -d: -f2)
    
    # 重新啟動 PostgreSQL
    echo ""
    echo "重新啟動 PostgreSQL..."
    execute_remote "sudo systemctl start postgresql@16-test" "啟動 PostgreSQL"
    sleep 5
    
    # 記錄結果
    local timestamp=$(date -Iseconds)
    
    # 寫入文字報告
    {
        echo ""
        echo "=========================================="
        echo "測試結果: $target_size"
        echo "=========================================="
        echo "時間: $timestamp"
        echo "Scale Factor: $scale_factor"
        echo "資料庫大小: $db_size"
        echo ""
        echo "備份階段:"
        echo "  耗時: ${backup_time} 秒"
        echo "  平均 CPU: ${backup_cpu}%"
        echo ""
        echo "還原階段:"
        echo "  耗時: ${restore_time} 秒"
        echo "  平均 CPU: ${restore_cpu}%"
        echo ""
    } >> "$RESULT_FILE"
    
    # 寫入 JSON (簡化版)
    if [ "$is_first" != "true" ]; then
        echo "," >> "$JSON_FILE"
    fi
    {
        echo "  {"
        echo "    \"target_size\": \"$target_size\","
        echo "    \"scale_factor\": $scale_factor,"
        echo "    \"database_size\": \"$db_size\","
        echo "    \"timestamp\": \"$timestamp\","
        echo "    \"backup_time\": $backup_time,"
        echo "    \"backup_avg_cpu\": $backup_cpu,"
        echo "    \"restore_time\": $restore_time,"
        echo "    \"restore_avg_cpu\": $restore_cpu"
        echo "  }"
    } >> "$JSON_FILE"
}

# 主程式
main() {
    local first=true
    
    for config in "${TEST_CONFIGS[@]}"; do
        IFS=':' read -r scale_factor target_size <<< "$config"
        
        if [ "$first" = true ]; then
            first=false
        else
            sleep 5
        fi
        
        run_test "$scale_factor" "$target_size" "$first"
    done
    
    # 完成 JSON
    echo "]" >> "$JSON_FILE"
    
    # 生成總結
    {
        echo ""
        echo "=========================================="
        echo "測試總結"
        echo "=========================================="
        echo "所有測試已完成！"
        echo "詳細結果請查看: $RESULT_FILE"
        echo "JSON 結果請查看: $JSON_FILE"
    } >> "$RESULT_FILE"
    
    echo ""
    echo -e "${GREEN}✓ 所有測試已完成！${NC}"
    echo "結果檔案: $RESULT_FILE"
    echo "JSON 檔案: $JSON_FILE"
}

# 執行主程式
main
