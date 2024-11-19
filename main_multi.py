# -*- encoding: utf-8 -*-
'''
@Time    :   2024/11/19 09:59:04
@Author  :   Li Zeng 
'''


import paramiko
import os
import configparser
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def read_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    
    servers = []
    for server_prefix in config.sections():
        if server_prefix.startswith('remote_server'):
            print(server_prefix)
            hostname = config[server_prefix]['hostname']
            port = int(config[server_prefix]['port'])
            username = config[server_prefix]['username']
            password = config[server_prefix]['password']
            remote_folder = config[server_prefix]['remote_folder']
            local_folder = config[server_prefix]['local_folder']
            max_workers = int(config[server_prefix]['max_workers'])
            
            servers.append((hostname, port, username, password, remote_folder, local_folder, max_workers))
    
    operation_mode = config['operation']['mode']
    
    return servers, operation_mode


def download_file_via_sftp(hostname, port, username, password, remote_file_path, local_file_path):
    try:
        # 每个任务创建自己的SSH和SFTP会话
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname, port, username, password)
            with ssh.open_sftp() as sftp:
                sftp.get(remote_file_path, local_file_path)
                print(f"Downloaded {remote_file_path} to {local_file_path}")
    except Exception as e:
        print(f"Failed to download {remote_file_path}: {e}")


def download_folder_via_sftp(server_info):
    print("download_folder_via_sftp")
    hostname, port, username, password, remote_folder, local_folder, max_workers = server_info
    # 使用with语句管理SSH和SFTP连接
    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname, port, username, password)
        
        with ssh.open_sftp() as sftp:
            # 确保本地文件夹存在
            if not os.path.exists(local_folder):
                os.makedirs(local_folder)
            
            # 遍历远程文件夹中的文件并准备下载任务
            remote_file_list = sftp.listdir(remote_folder)
            print(remote_file_list)
            tasks = []
            for file_name in remote_file_list:
                if ".py" in file_name:
                    remote_file_path = os.path.join(remote_folder, file_name)
                    local_file_path = os.path.join(local_folder, file_name)
                    tasks.append((remote_file_path, local_file_path))
            
            # 使用ThreadPoolExecutor并行下载文件，但每个任务管理自己的连接
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_task = {executor.submit(download_file_via_sftp, hostname, port, username, password, *task): task for task in tasks}
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Task generated an exception for file {task[2].split('/')[-1]}: {e}")


def ensure_remote_folder_exists(sftp, remote_folder_path):
    """确保远程文件夹存在，如果不存在则创建它"""
    try:
        sftp.listdir(remote_folder_path)  # 尝试列出目录内容以检查目录是否存在
    except FileNotFoundError:
        # 如果捕获到FileNotFoundError，则目录不存在，需要创建
        sftp.mkdir(remote_folder_path)
        print(f"Created remote folder: {remote_folder_path}")
    except IOError as e:
        # 捕获其他IO错误，可能是权限问题或其他
        print(f"Error accessing remote folder: {e}")
        raise


def upload_file_via_sftp(hostname, port, username, password, local_file_path, remote_file_path):
    try:
        # 每个任务创建自己的SSH和SFTP会话
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname, port, username, password)
            with ssh.open_sftp() as sftp:
                sftp.put(local_file_path, remote_file_path)
                print(f"Uploaded {local_file_path} to {remote_file_path}")
    except Exception as e:
        print(f"Failed to upload {local_file_path}: {e}")
 
 
def upload_folder_via_sftp(server_info):
    print("upload_folder_via_sftp")
    hostname, port, username, password, remote_folder, local_folder, max_workers = server_info
    # 使用with语句管理SSH和SFTP连接（注意：这里实际并不需要保持SSH连接打开，因为每个上传任务会独立打开）
    
    # 确保本地文件夹存在
    if not os.path.exists(local_folder):
        raise FileNotFoundError(f"Local folder {local_folder} does not exist.")
    
    try:
        # 每个任务创建自己的SSH和SFTP会话
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname, port, username, password)
            with ssh.open_sftp() as sftp:
                # 确保远程文件夹存在
                ensure_remote_folder_exists(sftp, os.path.dirname(remote_folder))
                print(f"ensure_remote_folder_exists: {remote_folder}")
    except Exception as e:
        print(f"Failed to ensure_remote_folder_exists: {remote_folder}: {e}")
    
    # 遍历本地文件夹中的文件并准备上传任务
    local_file_list = [f for f in os.listdir(local_folder) if os.path.isfile(os.path.join(local_folder, f))]
    print(local_file_list)
    tasks = []
    for file_name in local_file_list:
        if ".py" in file_name:  # 假设我们只上传.py文件，根据需要修改条件
            local_file_path = os.path.join(local_folder, file_name)
            remote_file_path = os.path.join(remote_folder, file_name)
            tasks.append((local_file_path, remote_file_path))
    
    # 使用ThreadPoolExecutor并行上传文件
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {executor.submit(upload_file_via_sftp, hostname, port, username, password, *task): task for task in tasks}
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                future.result()
            except Exception as e:
                print(f"Task generated an exception for file {task[0].split('/')[-1]}: {e}")


if __name__ == '__main__':
    # 读取配置文件中的参数
    config_file = './config_multi.ini'
    servers, operation_mode = read_config(config_file)
    print(servers)
    assert len(servers) > 0

    start_time = time.time()  # 记录开始时间
    
    with ThreadPoolExecutor(max_workers=len(servers)) as executor:  # 为每个服务器创建一个线程池
        future_to_server = {executor.submit(download_folder_via_sftp if operation_mode == 'download' else upload_folder_via_sftp, server): server for server in servers}
        for future in as_completed(future_to_server):
            server = future_to_server[future]
            try:
                future.result()
            except Exception as e:
                print(f"Failed to process server {server[0]}: {e}")
    
    end_time = time.time()  # 记录结束时间
    execution_time = end_time - start_time  # 计算执行时间
    print(f"All {len(servers)} servers {operation_mode} with {servers[0][-1]} max_workers processed in {execution_time:.4f} seconds.")

    # upload
    # All 2 servers upload with 3 max_workers processed in 13.0243 seconds.
    # All 2 servers upload with 5 max_workers processed in 10.0925 seconds.
    # All 2 servers upload with 10 max_workers processed in 5.2832 seconds.
    # All 2 servers upload with 20 max_workers processed in 3.5412 seconds.

    # download
    # All 2 servers download with 3 max_workers processed in 16.5736 seconds.
    # All 2 servers download with 5 max_workers processed in 9.4039 seconds.
    # All 2 servers download with 10 max_workers processed in 6.7719 seconds.
    # All 2 servers download with 20 max_workers processed in 5.0364 seconds.
    
    
