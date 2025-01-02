import os
import sys
import stat  # 添加这行导入
import paramiko
import hashlib
from tqdm import tqdm
import  re
import threading
from queue import Queue
from paramiko.ssh_exception import SSHException, AuthenticationException

class FileTransfer:
    def __init__(self, hostname, username, password,local_dir,remote_dir, port=12699, max_workers=5):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.ssh = None
        self.sftp = None
        self.task_queue = Queue()
        self.lock = threading.Lock()
        self.total_files = 0
        self.completed_files = 0
        self.max_workers = max_workers
        self.local_dir=local_dir
        self.remote_dir=remote_dir
    def connect(self):
        """建立SSH连接"""
        try:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(self.hostname, self.port, self.username, self.password)
        except AuthenticationException as e:
            print(f"认证失败: {str(e)}")
        except SSHException as e:
            print(f"SSH连接失败: {str(e)}")
        except Exception as e:
            print(f"连接失败: {str(e)}")
            sys.exit(1)

    def close(self):
        """关闭连接"""
        if self.sftp:
            self.sftp.close()
        if self.ssh:
            self.ssh.close()


    def reconnect(self):
        """重新连接服务器"""
        try:
            if self.sftp:
                self.sftp.close()
            if self.ssh:
                self.ssh.close()
            self.connect()
        except Exception as e:
            print(f"重新连接失败: {str(e)}")
            raise

    def sanitize_filename(self, filename):
        """清理文件名，替换非法字符"""
        replacements = {
            '[': '(',
            ']': ')',
            ':': '_',
            ' ': '_',
            ',': '.',
            '<': '',
            '>': '',
            '"': '',
            '/': '_',
            '\\': '_',
            '|': '_',
            '?': '',
            '*': '',
        }

        for old, new in replacements.items():
            filename = filename.replace(old, new)

        filename = re.sub(r'_+', '_', filename)
        filename = filename.strip('. ')

        # 处理 Windows 保留文件名
        reserved_names = {
            'CON', 'PRN', 'AUX', 'NUL',
            'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
            'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
        }
        name_without_ext = os.path.splitext(filename)[0].upper()
        if name_without_ext in reserved_names:
            filename = f"_{filename}"

        filename = filename[:255]
        return filename

    def normalize_path(self, path, is_windows=True):
        """统一路径格式并处理特殊字符"""
        if not is_windows:
            return path.replace('\\', '/')

        # 处理 UNC 路径
        if path.startswith('\\\\'):
            # 保持 UNC 路径的前两个反斜杠
            server_share = path.split('\\')[2:4]  # 获取服务器名和共享名
            remaining_path = path.split('\\')[4:]  # 获取剩余路径部分

            # 清理服务器名和共享名
            clean_server_share = [part for part in server_share if part]

            # 清理剩余路径部分
            clean_remaining = [self.sanitize_filename(part) for part in remaining_path if part]

            # 重新组合完整路径
            clean_path = '\\\\' + '\\'.join(clean_server_share)
            if clean_remaining:
                clean_path = clean_path + '\\' + '\\'.join(clean_remaining)
        else:
            # 处理本地路径
            parts = path.split('\\')

            # 如果第一部分包含盘符（如 "C:"），特殊处理
            if ':' in parts[0]:
                drive = parts[0]  # 保留盘符
                remaining_parts = parts[1:]
                clean_parts = [drive] + [self.sanitize_filename(part) for part in remaining_parts if part]
            else:
                clean_parts = [self.sanitize_filename(part) for part in parts if part]

            clean_path = '\\'.join(clean_parts)

        # 处理长路径
        if len(clean_path) > 250:
            if not clean_path.startswith('\\\\?\\'):
                if clean_path.startswith('\\\\'):
                    clean_path = '\\\\?\\UNC\\' + clean_path[2:]
                else:
                    clean_path = '\\\\?\\' + clean_path

        # 检查最终路径长度
        if len(clean_path) > 32767:
            raise ValueError("路径长度超过 Windows 的最大限制")

        return clean_path

    def create_sftp_client(self):
        """为每个线程创建独立的SFTP客户端"""
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.hostname, self.port, self.username, self.password)
        return ssh.open_sftp()

    def add_task_to_queue(self, remote_path, local_path, remote_size):
        """检查文件是否已经添加到队列"""
        task = (remote_path, local_path, remote_size)
        if task not in self.task_queue.queue:
            self.task_queue.put(task)
            with self.lock:
                self.total_files += 1

    def producer(self, remote_dir):
        """生产者：扫描远程目录并添加任务到队列"""
        try:
            sftp = self.create_sftp_client()
            self._scan_remote_dir(sftp, remote_dir, self.local_dir)
            sftp.close()
        except Exception as e:
            print(f"扫描目录出错: {str(e)}")
        finally:
            # 添加结束标记
            for _ in range(self.max_workers):
                self.task_queue.put(None)

    def _scan_remote_dir(self, sftp, remote_dir, local_dir):
        """递归扫描远程目录，添加文件大小检查"""
        try:
            for entry in sftp.listdir_attr(remote_dir):
                remote_path = self.normalize_path(f"{remote_dir}/{entry.filename}", False)
                local_path = self.normalize_path(os.path.join(local_dir, entry.filename), True)

                if stat.S_ISDIR(entry.st_mode):
                    os.makedirs(local_path, exist_ok=True)
                    self._scan_remote_dir(sftp, remote_path, local_path)
                else:
                    # 检查文件是否需要下载
                    remote_size = entry.st_size
                    need_download = True

                    if os.path.exists(local_path):
                        local_size = os.path.getsize(local_path)
                        if local_size == remote_size:
                            print(f"文件已存在且大小一致，跳过: {local_path}")
                            need_download = False

                    if need_download:
                        with self.lock:
                            self.total_files += 1
                        self.add_task_to_queue(remote_path, local_path, remote_size)

        except Exception as e:
            print(f"扫描目录 {remote_dir} 时出错: {str(e)}")

    def verify_file_integrity(self, sftp, remote_path, local_path):
        """验证文件完整性"""
        try:
            remote_size = sftp.stat(remote_path).st_size
            local_size = os.path.getsize(local_path)

            if remote_size != local_size:
                print(f"文件大小不匹配 {local_path}")
                print(f"远程大小: {remote_size} bytes")
                print(f"本地大小: {local_size} bytes")
                return False

            # 文件大于256MB才进行MD5校验，以提高速度
            if remote_size > 256 * 1024 * 1024:
                # 可选：计算文件的前1MB的MD5作为快速验证
                def calculate_partial_md5(file_path, chunk_size=1024 * 1024):
                    md5 = hashlib.md5()
                    with open(file_path, 'rb') as f:
                        data = f.read(chunk_size)
                        md5.update(data)
                    return md5.hexdigest()

                # 计算远程文件的部分MD5
                with sftp.open(remote_path, 'rb') as remote_file:
                    remote_md5 = hashlib.md5()
                    remote_md5.update(remote_file.read(1024 * 1024))
                    remote_hash = remote_md5.hexdigest()

                # 计算本地文件的部分MD5
                local_hash = calculate_partial_md5(local_path)

                if remote_hash != local_hash:
                    print(f"文件内容校验失败 {local_path}")
                    print(f"远程MD5: {remote_hash}")
                    print(f"本地MD5: {local_hash}")
                    return False

            return True

        except Exception as e:
            print(f"验证文件完整性时出错 {local_path}: {str(e)}")
            return False

    def download_file(self, sftp, remote_path, local_path):
        """下载单个文件，支持断点续传和完整性验证"""
        try:
            remote_size = sftp.stat(remote_path).st_size
            local_size = 0
            mode = 'wb'
            max_retries = 3
            retry_count = 0

            # 检查本地文件
            if os.path.exists(local_path):
                local_size = os.path.getsize(local_path)
                if local_size == remote_size:
                    # 验证文件完整性
                    if self.verify_file_integrity(sftp, remote_path, local_path):
                        print(f"文件已完成下载且验证通过: {local_path}")
                        return
                    else:
                        print(f"文件大小相同但验证失败，重新下载: {local_path}")
                        mode = 'wb'
                        local_size = 0
                elif local_size < remote_size:
                    mode = 'ab'
                    print(f"继续下载文件: {local_path} ({local_size}/{remote_size} bytes)")
                else:
                    mode = 'wb'
                    local_size = 0
                    print(f"本地文件大小异常，重新下载: {local_path}")

            # 创建目录（如果不存在）
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            while retry_count < max_retries:
                try:
                    # with self.lock:
                    with tqdm(total=remote_size, initial=local_size,
                              desc=os.path.basename(remote_path), unit='B',
                              unit_scale=True, leave=True, ncols=100) as pbar:
                        # 确保进度条在多次下载过程中不会破坏

                        with open(local_path, mode) as f:
                            if mode == 'ab':
                                f.seek(local_size)
                            sftp.get(remote_path, local_path,
                                     callback=lambda x, y: pbar.update(y - pbar.n))

                    # 下载完成后验证文件完整性
                    if self.verify_file_integrity(sftp, remote_path, local_path):
                        print(f"文件下载完成且验证通过: {local_path}")
                        return
                    else:
                        retry_count += 1
                        print(f"文件验证失败，尝试重新下载 ({retry_count}/{max_retries})")
                        mode = 'wb'
                        local_size = 0
                        continue

                except Exception as e:
                    retry_count += 1
                    print(f"下载出错，尝试重试 ({retry_count}/{max_retries})")
                    print(f"错误详细信息: {str(e)}")
                    if isinstance(e, SSHException):
                        print("SSH连接错误，检查网络连接或服务器配置。")
                    elif isinstance(e, FileNotFoundError):
                        print("文件未找到，可能是路径错误。")
                    else:
                        print(f"其他错误: {str(e)}")
                    if retry_count >= max_retries:
                        raise Exception(f"最大重试次数达到，文件下载失败。")
                    if retry_count < max_retries:
                        continue
                    raise

            raise Exception(f"达到最大重试次数，文件下载失败")

        except Exception as e:
            raise Exception(f"下载文件失败 {remote_path}: {str(e)}")

    def consumer(self,worker_id):
        """消费者：处理下载任务"""
        sftp = self.create_sftp_client()
        print(f"消费者线程 {worker_id} 启动")
        while True:
            task = self.task_queue.get()
            if task is None:  # 结束标记
                self.task_queue.task_done()
                break

            remote_path, local_path, file_size= task
            try:
                self.download_file(sftp, remote_path, local_path)
                with self.lock:
                    self.completed_files += 1
                    self._print_progress()
            except Exception as e:
                print(f"下载文件 {remote_path} 时出错: {str(e)}")
            finally:
                self.task_queue.task_done()

        sftp.close()
        print("已关闭SFTP连接，消费者线程结束")

    def _print_progress(self):
        """打印总体进度"""
        progress = (self.completed_files / self.total_files) * 100 if self.total_files > 0 else 0
        print(f"\r总进度: {progress:.2f}% ({self.completed_files}/{self.total_files})", end="")

    def start_transfer(self):
        """开始传输过程"""

        os.makedirs(self.local_dir, exist_ok=True)

        # 启动生产者线程
        producer_thread = threading.Thread(target=self.producer, args=(self.remote_dir,))
        producer_thread.start()

        # 启动消费者线程池
        consumers = []
        for _ in range(self.max_workers):
            consumer = threading.Thread(target=self.consumer,args=(_,))
            consumer.start()
            consumers.append(consumer)

        # 等待所有线程完成
        producer_thread.join()
        for consumer in consumers:
            consumer.join()

        print("\n传输完成!")

def main():
    # 连接配置
    hostname = ""
    username = ""
    password = ""
    remote_dirs = []
    local_dirs = []
    port=22
    max_workers=10

    # 创建传输对象并执行下载
    for index in range(len(remote_dirs)):
        transfer = FileTransfer(hostname, username, password,local_dir=local_dirs[index],remote_dir=remote_dirs[index],max_workers=max_workers,port=port)
        transfer.connect()
        try:
            transfer.start_transfer()
            print("传输完成")
        finally:
            transfer.close()

if __name__ == "__main__":
    main()
