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
        執行命令並監控 CPU 使用率和時間
        
        Returns:
            包含時間和 CPU 使用率的字典
        """
        print(f"\n開始執行: {description}")
        
        # 使用更可靠的方法監控 CPU（使用 vmstat 或 top）
        # 創建臨時監控腳本
        monitor_script = """
#!/bin/bash
LOG_FILE=/tmp/cpu_monitor_$$.log
while true; do
    if command -v vmstat >/dev/null 2>&1; then
        vmstat 1 1 | tail -1 | awk '{print 100-$15}'
    else
        top -bn1 | grep 'Cpu(s)' | awk '{print $2}' | sed 's/%us,//'
    fi
    sleep 1
done > "$LOG_FILE" 2>&1 &
echo $!
echo "$LOG_FILE"
"""
        
        # 上傳並執行監控腳本
        monitor_setup = self.execute_command(
            f"cat > /tmp/start_monitor.sh << 'EOF'\n{monitor_script}\nEOF\nchmod +x /tmp/start_monitor.sh"
        )
        monitor_output, _, _ = self.execute_command("/tmp/start_monitor.sh")
        monitor_lines = monitor_output.strip().split('\n')
        monitor_pid = monitor_lines[0] if monitor_lines else ""
        log_file = monitor_lines[1] if len(monitor_lines) > 1 else "/tmp/cpu_monitor.log"
        
        # 記錄開始時間和初始 CPU
        start_time = time.time()
        start_cpu = self.get_cpu_usage()
        
        # 執行主要命令（如果是 sudo 命令，使用 execute_sudo_command）
        if command.strip().startswith('sudo'):
            stdout, stderr, exit_code = self.execute_sudo_command(command, timeout=3600)
        else:
            stdout, stderr, exit_code = self.execute_command(command, timeout=3600)
        
        # 停止 CPU 監控
        if monitor_pid:
            self.execute_command(f"kill {monitor_pid} 2>/dev/null; wait {monitor_pid} 2>/dev/null")
        
        # 計算執行時間
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # 讀取 CPU 監控數據（平均值和峰值）
        cpu_avg_cmd = f"cat {log_file} 2>/dev/null | awk '{{sum+=$1; count++}} END {{if(count>0) print sum/count; else print 0}}'"
        cpu_max_cmd = f"cat {log_file} 2>/dev/null | awk '{{if($1>max || max==\"\") max=$1}} END {{print max+0}}'"
        
        cpu_avg_stdout, _, _ = self.execute_command(cpu_avg_cmd)
        cpu_max_stdout, _, _ = self.execute_command(cpu_max_cmd)
        
        try:
            avg_cpu = float(cpu_avg_stdout.strip())
        except:
            avg_cpu = self.get_cpu_usage()
        
        try:
            max_cpu = float(cpu_max_stdout.strip())
        except:
            max_cpu = avg_cpu
        
        # 清理臨時檔案
        self.execute_command(f"rm -f {log_file} /tmp/start_monitor.sh 2>/dev/null")
        
        end_cpu = self.get_cpu_usage()
        
        result = {
            'description': description,
            'elapsed_time_seconds': elapsed_time,
            'elapsed_time_formatted': f"{int(elapsed_time // 60)}分{int(elapsed_time % 60)}秒",
            'start_cpu': start_cpu,
            'end_cpu': end_cpu,
            'avg_cpu': avg_cpu,
            'max_cpu': max_cpu,  # 新增 CPU 峰值
            'exit_code': exit_code,
            'stdout': stdout[:500] if stdout else "",  # 只保留前500字元
            'stderr': stderr[:500] if stderr else ""
        }
        
        if exit_code == 0:
            print(f"✓ 完成: {description} - 耗時: {result['elapsed_time_formatted']}, 平均 CPU: {avg_cpu:.2f}%, 峰值 CPU: {max_cpu:.2f}%")
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
        test_result['backup_max_cpu'] = backup_result['max_cpu']  # 新增備份 CPU 峰值
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
        test_result['restore_max_cpu'] = restore_result['max_cpu']  # 新增還原 CPU 峰值
        
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
            
            for result in self.results:
                f.write(f"\n{'='*80}\n")
                f.write(f"測試項目: {result['target_size']}\n")
                f.write(f"Scale Factor: {result['scale_factor']}\n")
                f.write(f"資料庫大小: {result.get('database_size', '未知')}\n")
                f.write(f"測試時間: {result['timestamp']}\n")
                f.write(f"{'='*80}\n\n")
                
                f.write("備份階段:\n")
                f.write(f"  耗時: {result.get('backup_time', 0):.2f} 秒\n")
                f.write(f"  平均 CPU 使用率: {result.get('backup_avg_cpu', 0):.2f}%\n")
                f.write(f"  峰值 CPU 使用率: {result.get('backup_max_cpu', 0):.2f}%\n\n")
                
                f.write("還原階段:\n")
                f.write(f"  耗時: {result.get('restore_time', 0):.2f} 秒\n")
                f.write(f"  平均 CPU 使用率: {result.get('restore_avg_cpu', 0):.2f}%\n")
                f.write(f"  峰值 CPU 使用率: {result.get('restore_max_cpu', 0):.2f}%\n\n")
            
            # 總結表格
            f.write("\n" + "="*80 + "\n")
            f.write("測試總結\n")
            f.write("="*80 + "\n")
            f.write(f"{'資料大小':<12} {'資料庫大小':<12} {'備份時間(秒)':<14} {'備份CPU平均(%)':<16} {'備份CPU峰值(%)':<16} {'還原時間(秒)':<14} {'還原CPU平均(%)':<16} {'還原CPU峰值(%)':<16}\n")
            f.write("-"*120 + "\n")
            
            for result in self.results:
                f.write(f"{result['target_size']:<12} "
                       f"{result.get('database_size', 'N/A'):<12} "
                       f"{result.get('backup_time', 0):<14.2f} "
                       f"{result.get('backup_avg_cpu', 0):<16.2f} "
                       f"{result.get('backup_max_cpu', 0):<16.2f} "
                       f"{result.get('restore_time', 0):<14.2f} "
                       f"{result.get('restore_avg_cpu', 0):<16.2f} "
                       f"{result.get('restore_max_cpu', 0):<16.2f}\n")
        
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
    parser.add_argument('--host', '-H', type=str, default='10.31.155.37',
                       help='SSH 主機名稱或 IP (預設: 10.31.155.37)')
    parser.add_argument('--user', '-u', type=str, default='cghadmin',
                       help='SSH 使用者名稱 (預設: cghadmin)')
    parser.add_argument('--password', '-p', type=str, default='cgH@Dmin2025',
                       help='SSH 密碼 (預設: cgH@Dmin2025)')
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
    
    args = parser.parse_args()
    
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
