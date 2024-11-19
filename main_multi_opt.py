# -*- encoding: utf-8 -*-
'''
@Time    :   2024/11/19 12:51:42
@Author  :   Li Zeng 
'''


import paramiko
import os
import configparser
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging

# 配置日志，同时输出到文件和控制台
logging.basicConfig(
    level=logging.INFO,  # 设置日志级别
    format='%(asctime)s - %(levelname)s - %(message)s',  # 设置日志格式
    filename='app.log',  # 指定日志文件名
    filemode='w',  # 'w' 表示每次运行程序时覆盖日志文件，'a' 表示追加到日志文件末尾
)
 
# 创建一个控制台日志处理器，并设置级别为INFO
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
 
# 创建一个日志格式器，并设置给控制台处理器
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
 
# 将控制台处理器添加到根日志记录器
logging.getLogger().addHandler(console_handler)

def read_config(config_file):
    logging.info("read config begin")

    config = configparser.ConfigParser()
    config.read(config_file)
    
    servers = []
    for server_prefix in config.sections():
        if server_prefix.startswith('remote_server'):
            logging.info(server_prefix)
            hostname = config[server_prefix]['hostname']
            port = int(config[server_prefix]['port'])
            username = config[server_prefix]['username']
            password = config[server_prefix]['password']
            remote_folder = config[server_prefix]['remote_folder']
            local_folder = config[server_prefix]['local_folder']
            max_workers = int(config[server_prefix]['max_workers'])
            
            servers.append((hostname, port, username, password, remote_folder, local_folder, max_workers))
    
    operation_mode = config['operation']['mode']

    logging.info("read config end")
    
    return servers, operation_mode

def ensure_remote_folder_exists(sftp, remote_folder_path):
    """确保远程文件夹存在，如果不存在则创建它"""
    try:
        sftp.listdir(remote_folder_path)  # 尝试列出目录内容以检查目录是否存在
    except FileNotFoundError:
        # 如果捕获到FileNotFoundError，则目录不存在，需要创建
        sftp.mkdir(remote_folder_path)
        logging.info(f"Created remote folder: {remote_folder_path}")
    except IOError as e:
        # 捕获其他IO错误，可能是权限问题或其他
        logging.error(f"Error accessing remote folder: {e}")
        raise

def ensure_local_folder_exists(local_folder):
    # 确保本地文件夹存在
    if not os.path.exists(local_folder):
        os.makedirs(local_folder)
        logging.info(f"makedirs {local_folder}")

def create_sftp_session(hostname, port, username, password):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, port, username, password)
    return ssh
 
def transfer_file(sftp_session, is_download, local_path, remote_path):
    sftp = sftp_session.open_sftp()
    try:
        if is_download:
            sftp.get(remote_path, local_path)
            logging.info(f"Downloaded {remote_path} to {local_path}")
        else:
            sftp.put(local_path, remote_path)
            logging.info(f"Uploaded {local_path} to {remote_path}")
    except Exception as e:
        logging.error(f"Failed to transfer {remote_path}: {e}")
    finally:
        sftp.close()

def process_files(server_info, is_download):
    hostname, port, username, password, remote_folder, local_folder, max_workers = server_info
    ssh = create_sftp_session(hostname, port, username, password)
    try:
        with ssh:
            with ssh.open_sftp() as sftp:
                if is_download:
                    ensure_local_folder_exists(local_folder)
                    remote_file_list = sftp.listdir(remote_folder)
                else:
                    ensure_remote_folder_exists(sftp, remote_folder)
                    local_file_list = [f for f in os.listdir(local_folder) if os.path.isfile(os.path.join(local_folder, f))]
 
                tasks = []
                if is_download:
                    for file_name in remote_file_list:
                        if ".py" in file_name:
                            tasks.append((os.path.join(local_folder, file_name), os.path.join(remote_folder, file_name)))
                else:
                    for file_name in local_file_list:
                        if ".py" in file_name:
                            tasks.append((os.path.join(local_folder, file_name), os.path.join(remote_folder, file_name)))

                failed_tasks = set()

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_task = {executor.submit(transfer_file, ssh, is_download, *task): task for task in tasks}
                    for future in as_completed(future_to_task):
                        task = future_to_task[future]
                        try:
                            future.result()
                            logging.info("transfer file successful")
                        except Exception as e:
                            logging.error(f"Task generated an exception for file {task[1].split('/')[-1] if is_download else task[0].split('/')[-1]}: {e}")
                            failed_tasks.add(task)
                
                logging.info(f"failed_tasks size is {len(failed_tasks)}")
                logging.info(f"{failed_tasks}")

                # 记录传输失败文件并在减小max_workers后自动重传3次
                reload_cnt = 0
                while len(failed_tasks) > 0 and reload_cnt < 3:
                    logging.info("reloading")
                    max_workers = max_workers // 2
                    logging.info(f"current workers: {max_workers}")
                    with ThreadPoolExecutor(max_workers=max(max_workers, 1)) as executor:
                        future_to_task = {executor.submit(transfer_file, ssh, is_download, *task): task for task in failed_tasks}
                        for future in as_completed(future_to_task):
                            task = future_to_task[future]
                            try:
                                future.result()
                                logging.info("transfer file successful")
                                failed_tasks.remove(task)
                            except Exception as e:
                                logging.error(f"Task generated an exception for file {task[1].split('/')[-1] if is_download else task[0].split('/')[-1]}: {e}")

                    reload_cnt = reload_cnt + 1
                
                logging.info(f"failed_tasks size is {len(failed_tasks)}")
                logging.info(f"{failed_tasks}")

    finally:
        ssh.close()

if __name__ == '__main__':
    # 读取配置文件中的参数
    config_file = './cfg_multi.ini'
    servers, operation_mode = read_config(config_file)
    logging.info(f"{servers}")
    assert len(servers) > 0

    start_time = time.time()  # 记录开始时间
    
    with ThreadPoolExecutor(max_workers=len(servers)) as executor:  # 为每个服务器创建一个线程池
        future_to_server = {executor.submit(process_files, server, operation_mode == 'download'): server for server in servers}
        for future in as_completed(future_to_server):
            server = future_to_server[future]
            try:
                future.result()
                logging.info("process file successful")
            except Exception as e:
                print(f"Failed to process server {server[0]}: {e}")
    
    end_time = time.time()  # 记录结束时间
    execution_time = end_time - start_time  # 计算执行时间
    print(f"All {len(servers)} servers {operation_mode} with {servers[0][-1]} max_workers processed in {execution_time:.4f} seconds.")

    # upload
    # All 2 servers upload with 3 max_workers processed in 11.2273 seconds.
    # All 2 servers upload with 5 max_workers processed in 7.1863 seconds.
    # All 2 servers upload with 10 max_workers processed in xxx seconds.
    # All 2 servers upload with 20 max_workers processed in xxx seconds.

    # download
    # All 2 servers download with 3 max_workers processed in 16.2739 seconds.
    # All 2 servers download with 5 max_workers processed in 10.6489 seconds.
    # All 2 servers download with 10 max_workers processed in xxx seconds.
    # All 2 servers download with 20 max_workers processed in xxx seconds.
    
    