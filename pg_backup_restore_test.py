#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PostgreSQL 備份還原自動化測試腳本
自動化執行不同資料大小的備份還原測試，並記錄 CPU 使用率和時間
"""

import paramiko
import time
import re
import json
from datetime import datetime
from typing import Dict, List, Tuple
import sys
import argparse
import getpass
import os

class PostgreSQLTestAutomation:
    def __init__(self, hostname: str, username: str, password: str = None, 
                 key_file: str = None, port: int = 22, sudo_password: str = None,
                 process_max: int = None, archive_timeout: int = None):
        """
        初始化 SSH 連線
        
        Args:
            hostname: 遠端主機名稱或 IP
            username: SSH 使用者名稱
            password: SSH 密碼（如果使用密碼認證）
            key_file: SSH 私鑰檔案路徑（如果使用金鑰認證）
            port: SSH 連線埠號
            sudo_password: sudo 密碼（如果未提供，則使用 SSH 密碼）
            process_max: pgbackrest 最大進程數（用於並發備份）
            archive_timeout: pgbackrest 歸檔超時時間（秒）
        """
        self.hostname = hostname
        self.username = username
        self.sudo_password = sudo_password or password  # 如果沒指定，使用 SSH 密碼
        self.process_max = process_max
        self.archive_timeout = archive_timeout
        self.ssh_client = None
        self.results = []
        self.cpu_info = {}  # 儲存 CPU 規格資訊
        
        # 建立 SSH 連線
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            if key_file:
                self.ssh_client.connect(hostname, port=port, username=username, 
                                      key_filename=key_file, timeout=30)
            elif password:
                self.ssh_client.connect(hostname, port=port, username=username, 
                                      password=password, timeout=30)
            else:
                raise ValueError("必須提供 password 或 key_file")
            print(f"✓ 成功連線到 {hostname}")
            
            # 獲取系統 CPU 規格資訊
            self.cpu_info = self.get_cpu_info()
            if self.cpu_info:
                print(f"✓ 系統 CPU 規格: {self.cpu_info.get('model_name', '未知')}")
                print(f"  CPU 核心數: {self.cpu_info.get('cpu_cores', '未知')}")
                print(f"  邏輯 CPU 數: {self.cpu_info.get('logical_cpus', '未知')}")
        except Exception as e:
            print(f"✗ SSH 連線失敗: {e}")
            sys.exit(1)
    
    def execute_command(self, command: str, timeout: int = 300) -> Tuple[str, str, int]:
        """
        執行遠端命令
        
        Returns:
            (stdout, stderr, exit_code)
        """
        try:
            stdin, stdout, stderr = self.ssh_client.exec_command(command, timeout=timeout)
            exit_code = stdout.channel.recv_exit_status()
            stdout_text = stdout.read().decode('utf-8', errors='ignore')
            stderr_text = stderr.read().decode('utf-8', errors='ignore')
            return stdout_text, stderr_text, exit_code
        except Exception as e:
            return "", str(e), -1
    
    def execute_sudo_command(self, command: str, timeout: int = 300) -> Tuple[str, str, int]:
        """
        執行需要 sudo 的命令（使用 sudo -S 從標準輸入讀取密碼）
        
        Returns:
            (stdout, stderr, exit_code)
        """
        if self.sudo_password:
            # 轉義特殊字元，使用單引號包圍密碼
            # 將單引號轉義為 '\''
            escaped_password = self.sudo_password.replace("'", "'\"'\"'")
            # 使用 echo 將密碼傳給 sudo -S
            full_command = f"echo '{escaped_password}' | sudo -S {command}"
        else:
            # 如果沒有提供 sudo 密碼，嘗試直接執行（可能已配置 NOPASSWD）
            full_command = f"sudo {command}"
        
        return self.execute_command(full_command, timeout)
    
    def get_cpu_info(self) -> Dict:
        """
        獲取系統 CPU 規格資訊
        
        Returns:
            包含 CPU 資訊的字典
        """
        cpu_info = {}
        
        # 獲取 CPU 型號
        model_cmd = "grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | sed 's/^[[:space:]]*//'"
        model_stdout, _, _ = self.execute_command(model_cmd, timeout=10)
        if model_stdout.strip():
            cpu_info['model_name'] = model_stdout.strip()
        
        # 獲取物理 CPU 核心數
        cores_cmd = "grep -c '^processor' /proc/cpuinfo"
        cores_stdout, _, _ = self.execute_command(cores_cmd, timeout=10)
        try:
            logical_cpus = int(cores_stdout.strip())
            cpu_info['logical_cpus'] = logical_cpus
        except:
            cpu_info['logical_cpus'] = '未知'
        
        # 獲取物理核心數（如果有超線程，邏輯 CPU = 物理核心 * 2）
        physical_cores_cmd = "lscpu 2>/dev/null | grep '^CPU(s):' | awk '{print $2}'"
        physical_cores_stdout, _, _ = self.execute_command(physical_cores_cmd, timeout=10)
        if physical_cores_stdout.strip():
            try:
                cpu_info['cpu_cores'] = int(physical_cores_stdout.strip())
            except:
                pass
        
        # 如果沒有獲取到物理核心數，嘗試其他方法
        if 'cpu_cores' not in cpu_info or cpu_info['cpu_cores'] == '未知':
            # 嘗試從 /proc/cpuinfo 獲取
            cores_cmd = "grep 'cpu cores' /proc/cpuinfo | head -1 | awk '{print $4}'"
            cores_stdout, _, _ = self.execute_command(cores_cmd, timeout=10)
            if cores_stdout.strip():
                try:
                    cores_per_socket = int(cores_stdout.strip())
                    sockets_cmd = "grep 'physical id' /proc/cpuinfo | sort -u | wc -l"
                    sockets_stdout, _, _ = self.execute_command(sockets_cmd, timeout=10)
                    if sockets_stdout.strip():
                        sockets = int(sockets_stdout.strip())
                        cpu_info['cpu_cores'] = cores_per_socket * sockets
                except:
                    pass
        
        # 如果還是沒有，使用邏輯 CPU 數作為估算
        if 'cpu_cores' not in cpu_info or cpu_info['cpu_cores'] == '未知':
            if 'logical_cpus' in cpu_info and isinstance(cpu_info['logical_cpus'], int):
                # 假設有超線程，物理核心 = 邏輯 CPU / 2
                cpu_info['cpu_cores'] = cpu_info['logical_cpus'] // 2
            else:
                cpu_info['cpu_cores'] = '未知'
        
        return cpu_info
    
    def get_cpu_usage(self) -> float:
        """
        獲取當前 CPU 使用率（使用 top 命令）
        """
        command = "top -bn1 | grep 'Cpu(s)' | sed 's/.*, *\\([0-9.]*\\)%* id.*/\\1/' | awk '{print 100 - $1}'"
        stdout, stderr, exit_code = self.execute_command(command, timeout=10)
        try:
            cpu_usage = float(stdout.strip())
            return cpu_usage
        except:
            return 0.0
    
    def monitor_command_with_cpu(self, command: str, description: str) -> Dict:
        """
        執行命令並監控 CPU 使用率、IO 使用率和時間
        針對特定進程（pgbackrest）進行監控，而非系統整體
        
        Returns:
            包含時間、CPU 使用率和 IO 使用率的字典
        """
        print(f"\n開始執行: {description}")
        
        # 從命令中提取要監控的進程名稱
        # 例如：pgbackrest -> pgbackrest, pgbench -> pgbench
        process_name = "pgbackrest"
        if "pgbench" in command:
            process_name = "pgbench"
        elif "pgbackrest" in command:
            process_name = "pgbackrest"
        
        # 創建監控腳本，監控特定進程及其子進程
        # 使用普通字符串，然後替換 PROCESS_NAME，避免 f-string 的變量轉義問題
        monitor_script = """#!/bin/bash
CPU_LOG=/tmp/cpu_monitor_$$.log
IO_LOG=/tmp/io_monitor_$$.log
PROCESS_NAME="{PROCESS_NAME}"

# 函數：獲取指定進程的所有 PID（包括子進程）
get_process_pids() {{
    local main_pids=$(pgrep -f "$PROCESS_NAME" 2>/dev/null)
    local all_pids=""
    
    if [ -z "$main_pids" ]; then
        echo ""
        return 0
    fi
    
    for pid in $main_pids; do
        if [ ! -d "/proc/$pid" ]; then
            continue
        fi
        all_pids="$all_pids $pid"
        
        # 獲取該進程的直接子進程
        if command -v pgrep >/dev/null 2>&1; then
            local children=$(pgrep -P $pid 2>/dev/null)
            if [ -n "$children" ]; then
                all_pids="$all_pids $children"
                # 遞歸查找子進程的子進程（最多2層）
                for child in $children; do
                    if [ -d "/proc/$child" ]; then
                        local grandchildren=$(pgrep -P $child 2>/dev/null)
                        if [ -n "$grandchildren" ]; then
                            all_pids="$all_pids $grandchildren"
                        fi
                    fi
                done
            fi
        fi
    done
    
    # 去重並輸出
    echo "$all_pids" | tr ' ' '\\n' | sort -un | grep -v '^$' | tr '\\n' ' '
}}

# CPU 監控進程 - 監控特定進程的 CPU 使用率
(
    while true; do
        pids=$(get_process_pids)
        if [ -n "$pids" ]; then
            total_cpu=0
            cpu_count=0
            
            # 使用 pidstat 監控特定進程（如果可用，最準確）
            if command -v pidstat >/dev/null 2>&1; then
                for pid in $pids; do
                    if [ -d "/proc/$pid" ]; then
                        # pidstat -p PID 1 1 表示監控1秒，輸出1次
                        # 跳過標題行（前3行通常是標題），獲取數據行
                        pidstat_output=$(pidstat -p $pid 1 1 2>/dev/null)
                        if [ -n "$pidstat_output" ]; then
                            # 找到包含 PID 的數據行（跳過標題）
                            cpu_line=$(echo "$pidstat_output" | tail -n +4 | grep "^[[:space:]]*$pid[[:space:]]" | head -1)
                            if [ -z "$cpu_line" ]; then
                                # 如果沒找到，嘗試最後一行（可能是數據行）
                                cpu_line=$(echo "$pidstat_output" | tail -1)
                            fi
                            if [ -n "$cpu_line" ] && ! echo "$cpu_line" | grep -qE '^[[:space:]]*PID|Linux|^$'; then
                                # %CPU 通常在第 8 列，如果沒有則嘗試第 7 列
                                cpu=$(echo "$cpu_line" | awk '{{print $8}}' 2>/dev/null)
                                if [ -z "$cpu" ] || ! echo "$cpu" | grep -qE '^[0-9]+\\.?[0-9]*$'; then
                                    cpu=$(echo "$cpu_line" | awk '{{print $7}}' 2>/dev/null)
                                fi
                                # 驗證是數字
                                if [ -n "$cpu" ] && echo "$cpu" | grep -qE '^[0-9]+\\.?[0-9]*$'; then
                                    # 使用 awk 進行浮點數加法
                                    if [ -z "$total_cpu" ] || [ "$total_cpu" = "0" ]; then
                                        total_cpu="$cpu"
                                    else
                                        total_cpu=$(echo "$total_cpu $cpu" | awk '{{printf "%.2f", $1 + $2}}' 2>/dev/null)
                                    fi
                                    if [ $? -eq 0 ] && [ -n "$total_cpu" ]; then
                                        cpu_count=$((cpu_count + 1))
                                    fi
                                fi
                            fi
                        fi
                    fi
                done
            else
                # 使用 top 監控特定進程
                for pid in $pids; do
                    if [ -d "/proc/$pid" ]; then
                        cpu_line=$(top -bn1 -p $pid 2>/dev/null | tail -1)
                        if [ -n "$cpu_line" ]; then
                            cpu=$(echo "$cpu_line" | awk '{{print $9}}' 2>/dev/null)
                            # 驗證是數字（可能是整數或小數）
                            if [ -n "$cpu" ] && echo "$cpu" | grep -qE '^[0-9]+\\.?[0-9]*$'; then
                                # 使用 awk 進行浮點數加法
                                if [ -z "$total_cpu" ] || [ "$total_cpu" = "0" ]; then
                                    total_cpu="$cpu"
                                else
                                    total_cpu=$(echo "$total_cpu $cpu" | awk '{{printf "%.2f", $1 + $2}}' 2>/dev/null)
                                fi
                                if [ $? -eq 0 ] && [ -n "$total_cpu" ]; then
                                    cpu_count=$((cpu_count + 1))
                                fi
                            fi
                        fi
                    fi
                done
            fi
            
            if [ $cpu_count -gt 0 ] && [ -n "$total_cpu" ] && [ "$total_cpu" != "0" ]; then
                echo "$total_cpu"
            else
                echo "0"
            fi
        else
            echo "0"
        fi
        sleep 1
    done
) > "$CPU_LOG" 2>&1 &
CPU_PID=$!

# IO 監控進程 - 監控特定進程的 IO 速度（使用 pidstat -d）
(
    while true; do
        pids=$(get_process_pids)
        if [ -n "$pids" ]; then
            total_io_kb=0
            
            # 優先使用 pidstat -d 監控 IO（更準確且支持跨用戶進程）
            if command -v pidstat >/dev/null 2>&1; then
                for pid in $pids; do
                    if [ -d "/proc/$pid" ]; then
                        # pidstat -d 顯示 IO 統計，1 1 表示監控1秒，輸出1次
                        pidstat_output=$(pidstat -d -p $pid 1 1 2>/dev/null)
                        if [ -n "$pidstat_output" ]; then
                            # 找到數據行（跳過標題行，通常是前3行）
                            io_line=$(echo "$pidstat_output" | tail -n +4 | grep "^[[:space:]]*$pid[[:space:]]" | head -1)
                            if [ -z "$io_line" ]; then
                                # 如果沒找到，嘗試最後一行（可能是數據行）
                                io_line=$(echo "$pidstat_output" | tail -1)
                            fi
                            if [ -n "$io_line" ] && ! echo "$io_line" | grep -qE '^[[:space:]]*PID|Linux|^$'; then
                                # 讀取 kB_rd/s (第5列) 和 kB_wr/s (第6列)，單位是 kB/s
                                read_kb=$(echo "$io_line" | awk '{{print $5}}' 2>/dev/null)
                                write_kb=$(echo "$io_line" | awk '{{print $6}}' 2>/dev/null)
                                
                                # 驗證並累加讀取速度
                                if [ -n "$read_kb" ] && echo "$read_kb" | grep -qE '^[0-9]+\\.?[0-9]*$'; then
                                    total_io_kb=$(echo "$total_io_kb $read_kb" | awk '{{printf "%.0f", $1 + $2}}' 2>/dev/null)
                                fi
                                
                                # 驗證並累加寫入速度
                                if [ -n "$write_kb" ] && echo "$write_kb" | grep -qE '^[0-9]+\\.?[0-9]*$'; then
                                    total_io_kb=$(echo "$total_io_kb $write_kb" | awk '{{printf "%.0f", $1 + $2}}' 2>/dev/null)
                                fi
                            fi
                        fi
                    fi
                done
                
                # pidstat 已經給出 kB/s，直接輸出
                if [ -n "$total_io_kb" ] && [ "$total_io_kb" != "0" ]; then
                    echo "$total_io_kb"
                else
                    echo "0"
                fi
            else
                # 備用方案：使用 /proc/PID/io（需要 root 權限或進程屬於同一用戶）
                prev_total_read=0
                prev_total_write=0
                first_read=true
                
                curr_total_read=0
                curr_total_write=0
                
                for pid in $pids; do
                    if [ -f "/proc/$pid/io" ]; then
                        # 嘗試使用 sudo 讀取（如果可用）
                        read_bytes=$(sudo cat /proc/$pid/io 2>/dev/null | grep "^read_bytes:" | awk '{{print $2}}')
                        write_bytes=$(sudo cat /proc/$pid/io 2>/dev/null | grep "^write_bytes:" | awk '{{print $2}}')
                        if [ -z "$read_bytes" ]; then
                            # 如果 sudo 失敗，嘗試直接讀取
                            read_bytes=$(grep "^read_bytes:" /proc/$pid/io 2>/dev/null | awk '{{print $2}}')
                            write_bytes=$(grep "^write_bytes:" /proc/$pid/io 2>/dev/null | awk '{{print $2}}')
                        fi
                        if [ -n "$read_bytes" ] && echo "$read_bytes" | grep -qE '^[0-9]+$'; then
                            curr_total_read=$((curr_total_read + read_bytes))
                        fi
                        if [ -n "$write_bytes" ] && echo "$write_bytes" | grep -qE '^[0-9]+$'; then
                            curr_total_write=$((curr_total_write + write_bytes))
                        fi
                    fi
                done
                
                if [ "$first_read" = "true" ]; then
                    prev_total_read=$curr_total_read
                    prev_total_write=$curr_total_write
                    first_read=false
                    echo "0"
                else
                    if [ $curr_total_read -ge $prev_total_read ] && [ $curr_total_write -ge $prev_total_write ]; then
                        read_diff=$((curr_total_read - prev_total_read))
                        write_diff=$((curr_total_write - prev_total_write))
                        total_diff=$((read_diff + write_diff))
                        
                        if [ $total_diff -gt 0 ]; then
                            io_kb=$(echo "scale=0; $total_diff / 1024" | bc 2>/dev/null)
                            if [ -z "$io_kb" ] || [ "$io_kb" = "" ]; then
                                io_kb=$((total_diff / 1024))
                            fi
                            echo "$io_kb"
                        else
                            echo "0"
                        fi
                        
                        prev_total_read=$curr_total_read
                        prev_total_write=$curr_total_write
                    else
                        prev_total_read=$curr_total_read
                        prev_total_write=$curr_total_write
                        echo "0"
                    fi
                fi
            fi
        else
            # 進程不存在
            echo "0"
        fi
        sleep 1
    done
) > "$IO_LOG" 2>&1 &
IO_PID=$!

echo $CPU_PID
echo $IO_PID
echo "$CPU_LOG"
echo "$IO_LOG"
""".format(PROCESS_NAME=process_name)
        
        # 上傳並執行監控腳本
        # 使用 'EOF' 防止 bash 變量被解釋，但 PROCESS_NAME 已經在 Python 中替換了
        monitor_setup = self.execute_command(
            f"cat > /tmp/start_monitor.sh << 'MONITOR_EOF'\n{monitor_script}\nMONITOR_EOF\nchmod +x /tmp/start_monitor.sh"
        )
        monitor_output, _, _ = self.execute_command("/tmp/start_monitor.sh")
        monitor_lines = monitor_output.strip().split('\n')
        cpu_pid = monitor_lines[0] if len(monitor_lines) > 0 else ""
        io_pid = monitor_lines[1] if len(monitor_lines) > 1 else ""
        cpu_log_file = monitor_lines[2] if len(monitor_lines) > 2 else "/tmp/cpu_monitor.log"
        io_log_file = monitor_lines[3] if len(monitor_lines) > 3 else "/tmp/io_monitor.log"
        
        # 等待一小段時間讓監控進程啟動
        time.sleep(0.5)
        
        # 記錄開始時間和初始 CPU
        start_time = time.time()
        start_cpu = self.get_cpu_usage()
        
        # 執行主要命令（如果是 sudo 命令，使用 execute_sudo_command）
        if command.strip().startswith('sudo'):
            stdout, stderr, exit_code = self.execute_sudo_command(command, timeout=3600)
        else:
            stdout, stderr, exit_code = self.execute_command(command, timeout=3600)
        
        # 等待一小段時間確保最後的監控數據被記錄
        time.sleep(1)
        
        # 停止監控進程
        if cpu_pid:
            self.execute_command(f"kill {cpu_pid} 2>/dev/null; wait {cpu_pid} 2>/dev/null")
        if io_pid:
            self.execute_command(f"kill {io_pid} 2>/dev/null; wait {io_pid} 2>/dev/null")
        
        # 計算執行時間
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # 讀取 CPU 監控數據（平均值和峰值）
        cpu_avg_cmd = f"cat {cpu_log_file} 2>/dev/null | awk '{{sum+=$1; count++}} END {{if(count>0) print sum/count; else print 0}}'"
        cpu_max_cmd = f"cat {cpu_log_file} 2>/dev/null | awk '{{if($1>max || max==\"\") max=$1}} END {{print max+0}}'"
        
        cpu_avg_stdout, _, _ = self.execute_command(cpu_avg_cmd)
        cpu_max_stdout, _, _ = self.execute_command(cpu_max_cmd)
        
        try:
            avg_cpu = float(cpu_avg_stdout.strip())
        except:
            avg_cpu = 0.0
        
        try:
            max_cpu = float(cpu_max_stdout.strip())
        except:
            max_cpu = avg_cpu
        
        # 讀取 IO 監控數據（平均值和峰值，單位 kB/s）
        io_avg_cmd = f"cat {io_log_file} 2>/dev/null | awk '{{sum+=$1; count++}} END {{if(count>0) print sum/count; else print 0}}'"
        io_max_cmd = f"cat {io_log_file} 2>/dev/null | awk '{{if($1>max || max==\"\") max=$1}} END {{print max+0}}'"
        
        io_avg_stdout, _, _ = self.execute_command(io_avg_cmd)
        io_max_stdout, _, _ = self.execute_command(io_max_cmd)
        
        try:
            avg_io_kb = float(io_avg_stdout.strip())
        except:
            avg_io_kb = 0.0
        
        try:
            max_io_kb = float(io_max_stdout.strip())
        except:
            max_io_kb = avg_io_kb
        
        # 轉換為 MB/s 以便閱讀
        avg_io_mb = avg_io_kb / 1024.0
        max_io_mb = max_io_kb / 1024.0
        
        # 清理臨時檔案
        self.execute_command(f"rm -f {cpu_log_file} {io_log_file} /tmp/start_monitor.sh /tmp/target_pids_*.txt 2>/dev/null")
        
        end_cpu = self.get_cpu_usage()
        
        result = {
            'description': description,
            'elapsed_time_seconds': elapsed_time,
            'elapsed_time_formatted': f"{int(elapsed_time // 60)}分{int(elapsed_time % 60)}秒",
            'start_cpu': start_cpu,
            'end_cpu': end_cpu,
            'avg_cpu': avg_cpu,
            'max_cpu': max_cpu,
            'avg_io_kb': avg_io_kb,
            'max_io_kb': max_io_kb,
            'avg_io_mb': avg_io_mb,
            'max_io_mb': max_io_mb,
            'exit_code': exit_code,
            'stdout': stdout[:500] if stdout else "",  # 只保留前500字元
            'stderr': stderr[:500] if stderr else ""
        }
        
        if exit_code == 0:
            print(f"✓ 完成: {description} - 耗時: {result['elapsed_time_formatted']}, 平均 CPU: {avg_cpu:.2f}%, 峰值 CPU: {max_cpu:.2f}%, 平均 IO: {avg_io_mb:.2f} MB/s, 峰值 IO: {max_io_mb:.2f} MB/s")
        else:
            print(f"✗ 失敗: {description} - 退出碼: {exit_code}")
            if stderr:
                print(f"  錯誤訊息: {stderr[:200]}")
        
        return result
    
    def run_test_sequence(self, scale_factor: int, target_size: str):
        """
        執行完整的測試序列
        
        Args:
            scale_factor: pgbench scale factor
            target_size: 目標資料大小（用於記錄）
        """
        print(f"\n{'='*60}")
        print(f"開始測試: {target_size} (scale factor: {scale_factor})")
        print(f"{'='*60}")
        
        test_result = {
            'target_size': target_size,
            'scale_factor': scale_factor,
            'timestamp': datetime.now().isoformat(),
            'steps': []
        }
        
        # 步驟 1: 資料填充
        print("\n[步驟 1/6] 資料填充...")
        fill_result = self.monitor_command_with_cpu(
            f"sudo -u postgres pgbench -i -s {scale_factor} -p 5433 postgres",
            f"資料填充 ({target_size})"
        )
        test_result['steps'].append(fill_result)
        time.sleep(2)
        
        # 步驟 2: 記錄資料量大小
        print("\n[步驟 2/6] 記錄資料量大小...")
        size_stdout, size_stderr, size_exit = self.execute_sudo_command(
            "-u postgres psql -p 5433 -c \"SELECT pg_size_pretty(pg_database_size('postgres'));\""
        )
        if size_exit == 0:
            # 提取資料庫大小
            size_match = re.search(r'(\d+\.?\d*\s*\w+)', size_stdout)
            db_size = size_match.group(1) if size_match else "未知"
            print(f"✓ 資料庫大小: {db_size}")
            test_result['database_size'] = db_size
        else:
            print(f"✗ 無法取得資料庫大小")
            test_result['database_size'] = "錯誤"
        
        # 步驟 3: 備份（記錄時間與 CPU）
        print("\n[步驟 3/6] 執行備份...")
        # 構建備份命令，如果有 process_max 或 archive_timeout 參數則添加
        backup_cmd = "sudo -u postgres pgbackrest --stanza=test-backup --type=full"
        if self.process_max:
            backup_cmd += f" --process-max={self.process_max}"
        if self.archive_timeout:
            backup_cmd += f" --archive-timeout={self.archive_timeout}"
        backup_cmd += " backup"
        
        backup_result = self.monitor_command_with_cpu(
            backup_cmd,
            f"備份 ({target_size})"
        )
        test_result['steps'].append(backup_result)
        test_result['backup_time'] = backup_result['elapsed_time_seconds']
        test_result['backup_avg_cpu'] = backup_result['avg_cpu']
        test_result['backup_max_cpu'] = backup_result['max_cpu']
        test_result['backup_avg_io_mb'] = backup_result['avg_io_mb']
        test_result['backup_max_io_mb'] = backup_result['max_io_mb']
        time.sleep(2)
        
        # 步驟 4: 停止 PostgreSQL
        print("\n[步驟 4/6] 停止 PostgreSQL...")
        stop_stdout, stop_stderr, stop_exit = self.execute_sudo_command(
            "systemctl stop postgresql@16-test"
        )
        if stop_exit == 0:
            print("✓ PostgreSQL 已停止")
        else:
            print(f"✗ 停止 PostgreSQL 失敗: {stop_stderr[:200]}")
        time.sleep(2)
        
        # 步驟 5: 刪除資料目錄
        print("\n[步驟 5/6] 刪除資料目錄...")
        delete_stdout, delete_stderr, delete_exit = self.execute_sudo_command(
            "bash -c 'rm -rf /var/lib/postgresql/16/test/*'"
        )
        if delete_exit == 0:
            print("✓ 資料目錄已刪除")
        else:
            print(f"✗ 刪除資料目錄失敗: {delete_stderr[:200]}")
        time.sleep(2)
        
        # 步驟 6: 還原（計時與 CPU 使用率）
        print("\n[步驟 6/6] 執行還原...")
        restore_result = self.monitor_command_with_cpu(
            "sudo -u postgres pgbackrest --stanza=test-backup restore",
            f"還原 ({target_size})"
        )
        test_result['steps'].append(restore_result)
        test_result['restore_time'] = restore_result['elapsed_time_seconds']
        test_result['restore_avg_cpu'] = restore_result['avg_cpu']
        test_result['restore_max_cpu'] = restore_result['max_cpu']
        test_result['restore_avg_io_mb'] = restore_result['avg_io_mb']
        test_result['restore_max_io_mb'] = restore_result['max_io_mb']
        
        # 重新啟動 PostgreSQL（如果需要）
        print("\n重新啟動 PostgreSQL...")
        start_stdout, start_stderr, start_exit = self.execute_sudo_command(
            "systemctl start postgresql@16-test"
        )
        if start_exit == 0:
            print("✓ PostgreSQL 已啟動")
            time.sleep(5)  # 等待服務啟動
        else:
            print(f"✗ 啟動 PostgreSQL 失敗: {start_stderr[:200]}")
        
        self.results.append(test_result)
        return test_result
    
    def generate_report(self, output_file: str = "test_results.json"):
        """
        生成測試報告
        """
        # 保存 JSON 報告
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        
        # 生成文字報告
        report_file = output_file.replace('.json', '_report.txt')
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("PostgreSQL 備份還原測試報告\n")
            f.write("="*80 + "\n\n")
            
            # 顯示系統 CPU 規格資訊
            if self.cpu_info:
                f.write("系統資訊:\n")
                f.write(f"  CPU 型號: {self.cpu_info.get('model_name', '未知')}\n")
                f.write(f"  物理 CPU 核心數: {self.cpu_info.get('cpu_cores', '未知')}\n")
                f.write(f"  邏輯 CPU 數: {self.cpu_info.get('logical_cpus', '未知')}\n")
                if isinstance(self.cpu_info.get('cpu_cores'), int) and isinstance(self.cpu_info.get('logical_cpus'), int):
                    cpu_cores = self.cpu_info['cpu_cores']
                    logical_cpus = self.cpu_info['logical_cpus']
                    if logical_cpus > 0:
                        max_cpu_percent = (cpu_cores * 100)
                        f.write(f"  最大 CPU 使用率（相對於物理核心）: {max_cpu_percent}%\n")
                f.write("\n")
            
            for result in self.results:
                f.write(f"\n{'='*80}\n")
                f.write(f"測試項目: {result['target_size']}\n")
                f.write(f"Scale Factor: {result['scale_factor']}\n")
                f.write(f"資料庫大小: {result.get('database_size', '未知')}\n")
                f.write(f"測試時間: {result['timestamp']}\n")
                f.write(f"{'='*80}\n\n")
                
                f.write("備份階段:\n")
                f.write(f"  耗時: {result.get('backup_time', 0):.2f} 秒\n")
                backup_avg_cpu = result.get('backup_avg_cpu', 0)
                backup_max_cpu = result.get('backup_max_cpu', 0)
                f.write(f"  平均 CPU 使用率: {backup_avg_cpu:.2f}%")
                if isinstance(self.cpu_info.get('cpu_cores'), int) and self.cpu_info['cpu_cores'] > 0:
                    backup_avg_cpu_relative = (backup_avg_cpu / (self.cpu_info['cpu_cores'] * 100)) * 100
                    f.write(f" (相對於物理核心: {backup_avg_cpu_relative:.2f}%)")
                f.write("\n")
                f.write(f"  峰值 CPU 使用率: {backup_max_cpu:.2f}%")
                if isinstance(self.cpu_info.get('cpu_cores'), int) and self.cpu_info['cpu_cores'] > 0:
                    backup_max_cpu_relative = (backup_max_cpu / (self.cpu_info['cpu_cores'] * 100)) * 100
                    f.write(f" (相對於物理核心: {backup_max_cpu_relative:.2f}%)")
                f.write("\n")
                f.write(f"  平均 IO 速度: {result.get('backup_avg_io_mb', 0):.2f} MB/s\n")
                f.write(f"  峰值 IO 速度: {result.get('backup_max_io_mb', 0):.2f} MB/s\n\n")
                
                f.write("還原階段:\n")
                f.write(f"  耗時: {result.get('restore_time', 0):.2f} 秒\n")
                restore_avg_cpu = result.get('restore_avg_cpu', 0)
                restore_max_cpu = result.get('restore_max_cpu', 0)
                f.write(f"  平均 CPU 使用率: {restore_avg_cpu:.2f}%")
                if isinstance(self.cpu_info.get('cpu_cores'), int) and self.cpu_info['cpu_cores'] > 0:
                    restore_avg_cpu_relative = (restore_avg_cpu / (self.cpu_info['cpu_cores'] * 100)) * 100
                    f.write(f" (相對於物理核心: {restore_avg_cpu_relative:.2f}%)")
                f.write("\n")
                f.write(f"  峰值 CPU 使用率: {restore_max_cpu:.2f}%")
                if isinstance(self.cpu_info.get('cpu_cores'), int) and self.cpu_info['cpu_cores'] > 0:
                    restore_max_cpu_relative = (restore_max_cpu / (self.cpu_info['cpu_cores'] * 100)) * 100
                    f.write(f" (相對於物理核心: {restore_max_cpu_relative:.2f}%)")
                f.write("\n")
                f.write(f"  平均 IO 速度: {result.get('restore_avg_io_mb', 0):.2f} MB/s\n")
                f.write(f"  峰值 IO 速度: {result.get('restore_max_io_mb', 0):.2f} MB/s\n\n")
            
            # 總結表格
            f.write("\n" + "="*80 + "\n")
            f.write("測試總結\n")
            f.write("="*80 + "\n")
            f.write(f"{'資料大小':<12} {'資料庫大小':<12} {'備份時間(秒)':<14} {'備份CPU平均(%)':<16} {'備份CPU峰值(%)':<16} {'備份IO平均(MB/s)':<18} {'備份IO峰值(MB/s)':<18} {'還原時間(秒)':<14} {'還原CPU平均(%)':<16} {'還原CPU峰值(%)':<16} {'還原IO平均(MB/s)':<18} {'還原IO峰值(MB/s)':<18}\n")
            f.write("-"*200 + "\n")
            
            for result in self.results:
                f.write(f"{result['target_size']:<12} "
                       f"{result.get('database_size', 'N/A'):<12} "
                       f"{result.get('backup_time', 0):<14.2f} "
                       f"{result.get('backup_avg_cpu', 0):<16.2f} "
                       f"{result.get('backup_max_cpu', 0):<16.2f} "
                       f"{result.get('backup_avg_io_mb', 0):<18.2f} "
                       f"{result.get('backup_max_io_mb', 0):<18.2f} "
                       f"{result.get('restore_time', 0):<14.2f} "
                       f"{result.get('restore_avg_cpu', 0):<16.2f} "
                       f"{result.get('restore_max_cpu', 0):<16.2f} "
                       f"{result.get('restore_avg_io_mb', 0):<18.2f} "
                       f"{result.get('restore_max_io_mb', 0):<18.2f}\n")
        
        print(f"\n✓ 測試報告已生成:")
        print(f"  - JSON: {output_file}")
        print(f"  - 文字報告: {report_file}")
    
    def close(self):
        """關閉 SSH 連線"""
        if self.ssh_client:
            self.ssh_client.close()
            print("\n✓ SSH 連線已關閉")


def main():
    """
    主程式
    """
    parser = argparse.ArgumentParser(description='PostgreSQL 備份還原自動化測試')
    parser.add_argument('--host', '-H', type=str, default='127.0.0.1',
                       help='SSH 主機名稱或 IP (預設: 127.0.0.1，建議改由本機 config.json 設定)')
    parser.add_argument('--user', '-u', type=str, default='postgres',
                       help='SSH 使用者名稱 (預設: postgres，建議改由本機 config.json 設定)')
    parser.add_argument('--password', '-p', type=str, default='',
                       help='SSH 密碼（預設為空字串，建議改由本機 config.json 或互動模式輸入）')
    parser.add_argument('--key-file', '-k', type=str, default=None,
                       help='SSH 金鑰檔案路徑（如果使用金鑰認證）')
    parser.add_argument('--port', type=int, default=22,
                       help='SSH 連線埠號 (預設: 22)')
    parser.add_argument('--interactive', '-i', action='store_true',
                       help='使用互動式輸入（忽略命令列參數）')
    parser.add_argument('--auto-start', '-a', action='store_true',
                       help='自動開始測試，不需要確認')
    parser.add_argument('--sizes', '-s', type=str, default=None,
                       help='要測試的資料大小，用逗號分隔（例如: 1GB,3GB,5GB 或 500MB,2GB）。如果未指定，使用預設值：1GB,3GB,5GB,9GB')
    parser.add_argument('--process-max', type=int, default=None,
                       help='pgbackrest 最大進程數（用於並發備份，例如: 4）')
    parser.add_argument('--archive-timeout', type=int, default=None,
                       help='pgbackrest 歸檔超時時間（秒，例如: 300 表示 5 分鐘）')
    parser.add_argument('--config', type=str, default='config.json',
                       help='連線設定檔路徑（預設: config.json，如存在會覆蓋 --host/--user/--password/--port 等參數）')
    
    args = parser.parse_args()

    # 從本機設定檔載入連線資訊（不會被版控到 GitHub）
    config = {}
    if args.config and os.path.exists(args.config):
        try:
            with open(args.config, 'r', encoding='utf-8') as f:
                config = json.load(f)
        except Exception as e:
            print(f"⚠ 無法讀取設定檔 {args.config}: {e}")
            config = {}

    # 如果設定檔有提供值，覆蓋 argparse 預設 / 命令列值
    if isinstance(config, dict):
        if 'host' in config and config['host']:
            args.host = config['host']
        if 'user' in config and config['user']:
            args.user = config['user']
        if 'password' in config:
            # 允許密碼為空字串，但如果 key 存在就以設定檔為準
            args.password = config['password']
        if 'port' in config and config['port']:
            args.port = int(config['port'])
        if 'process_max' in config and config['process_max'] is not None:
            args.process_max = int(config['process_max'])
        if 'archive_timeout' in config and config['archive_timeout'] is not None:
            args.archive_timeout = int(config['archive_timeout'])
    
    print("PostgreSQL 備份還原自動化測試")
    print("="*60)
    
    # 如果使用互動式模式，則提示輸入
    if args.interactive:
        print("\n請輸入 SSH 連線資訊:")
        hostname = input(f"主機名稱或 IP [{args.host}]: ").strip() or args.host
        username = input(f"使用者名稱 [{args.user}]: ").strip() or args.user
        
        auth_method = input("認證方式 (1: 密碼, 2: 金鑰檔案) [1]: ").strip() or "1"
        password = None
        key_file = None
        
        if auth_method == "1":
            password = getpass.getpass(f"密碼 [使用預設]: ") or args.password
        elif auth_method == "2":
            key_file = input("金鑰檔案路徑: ").strip()
        else:
            print("無效的認證方式")
            sys.exit(1)
    else:
        # 使用命令列參數或預設值
        hostname = args.host
        username = args.user
        password = args.password
        key_file = args.key_file
        
        print(f"\n使用連線資訊:")
        print(f"  主機: {hostname}")
        print(f"  使用者: {username}")
        print(f"  認證方式: {'金鑰檔案' if key_file else '密碼'}")
    
    # 建立自動化物件（sudo 密碼與 SSH 密碼相同）
    automation = PostgreSQLTestAutomation(
        hostname=hostname,
        username=username,
        password=password,
        key_file=key_file,
        port=args.port,
        sudo_password=password,  # sudo 密碼與 SSH 密碼相同
        process_max=args.process_max,  # pgbackrest 最大進程數
        archive_timeout=args.archive_timeout  # pgbackrest 歸檔超時時間
    )
    
    # 顯示 pgbackrest 設定
    if args.process_max:
        print(f"pgbackrest 最大進程數: {args.process_max}")
    if args.archive_timeout:
        print(f"pgbackrest 歸檔超時時間: {args.archive_timeout} 秒 ({args.archive_timeout // 60} 分鐘)")
    
    try:
        # 定義測試項目
        # 根據實際測試結果，每個 scale factor ≈ 15 MB
        # 因此調整 scale factor 以達到目標資料大小：目標大小(MB) ÷ 15 ≈ scale factor
        
        def parse_size(size_str: str) -> int:
            """將大小字串（如 '1GB', '500MB'）轉換為 MB"""
            size_str = size_str.strip().upper()
            if size_str.endswith('GB'):
                return int(float(size_str[:-2]) * 1024)
            elif size_str.endswith('MB'):
                return int(float(size_str[:-2]))
            elif size_str.endswith('KB'):
                return int(float(size_str[:-2]) / 1024)
            else:
                # 假設是數字，單位為 MB
                return int(float(size_str))
        
        def calculate_scale_factor(target_mb: int) -> int:
            """根據目標大小（MB）計算 scale factor"""
            # 每個 scale factor ≈ 15 MB
            return max(1, int(target_mb / 15))
        
        # 處理用戶指定的資料大小
        if args.sizes:
            size_list = [s.strip() for s in args.sizes.split(',')]
            test_configs = []
            for size_str in size_list:
                target_mb = parse_size(size_str)
                scale_factor = calculate_scale_factor(target_mb)
                # 保持原始格式作為標籤
                test_configs.append((scale_factor, size_str))
            print(f"\n使用自訂測試大小: {args.sizes}")
        else:
            # 預設測試項目
            test_configs = [
                (68, "1GB"),
                (205, "3GB"),
                (341, "5GB"),
                (614, "9GB")
            ]
            print(f"\n使用預設測試大小: 1GB, 3GB, 5GB, 9GB")
        
        print(f"將執行 {len(test_configs)} 個測試項目...")
        for scale_factor, target_size in test_configs:
            target_mb = parse_size(target_size)
            print(f"  - {target_size} (scale factor: {scale_factor}, 預期約 {target_mb} MB)")
        
        if not args.auto_start:
            confirm = input("是否繼續? (y/n): ").strip().lower()
            if confirm != 'y':
                print("已取消")
                return
        else:
            print("自動開始測試...")
        
        # 執行測試
        for scale_factor, target_size in test_configs:
            automation.run_test_sequence(scale_factor, target_size)
            time.sleep(5)  # 測試間隔
        
        # 生成報告
        automation.generate_report()
        
    except KeyboardInterrupt:
        print("\n\n測試被使用者中斷")
    except Exception as e:
        print(f"\n✗ 發生錯誤: {e}")
        import traceback
        traceback.print_exc()
    finally:
        automation.close()


if __name__ == "__main__":
    main()
