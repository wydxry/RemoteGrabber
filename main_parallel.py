# -*- encoding: utf-8 -*-
'''
@Time    :   2024/11/18 16:14:53
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
    
    # 从配置文件中读取参数
    hostname = config['remote_server']['hostname']
    port = int(config['remote_server']['port'])
    username = config['remote_server']['username']
    password = config['remote_server']['password']
    remote_folder = config['remote_server']['remote_folder']
    local_folder = config['remote_server']['local_folder']
    max_workers = int(config['remote_server']['max_workers'])
    operation_mode = int(config['remote_server']['operation_mode'])
    
    return hostname, port, username, password, remote_folder, local_folder, max_workers, operation_mode


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


def download_folder_via_sftp(hostname, port, username, password, remote_folder, local_folder, max_workers=5):
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
            tasks = []
            for file_name in remote_file_list:
                if ".py" in file_name:
                    remote_file_path = os.path.join(remote_folder, file_name)
                    local_file_path = os.path.join(local_folder, file_name)
                    # tasks.append((sftp, remote_file_path, local_file_path))
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
 
 
def upload_folder_via_sftp(hostname, port, username, password, local_folder, remote_folder, max_workers=5):
    # 使用with语句管理SSH和SFTP连接（注意：这里实际并不需要保持SSH连接打开，因为每个上传任务会独立打开）
    
    # 确保本地文件夹存在
    if not os.path.exists(local_folder):
        raise FileNotFoundError(f"Local folder {local_folder} does not exist.")
    
    # 遍历本地文件夹中的文件并准备上传任务
    local_file_list = [f for f in os.listdir(local_folder) if os.path.isfile(os.path.join(local_folder, f))]
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
    config_file = './config.ini'
    hostname, port, username, password, remote_folder, local_folder, max_workers, operation_mode = read_config(config_file)

    start_time = time.time()  # 记录开始时间
    
    if operation_mode == 1:
        # 调用上传函数
        upload_folder_via_sftp(hostname, port, username, password, local_folder, remote_folder, max_workers)
    elif operation_mode == 2:
        # 调用下载函数
        download_folder_via_sftp(hostname, port, username, password, remote_folder, local_folder, max_workers)
    else:
        print("Invalid operation mode. Please set 'operation_mode' to either 'upload' or 'download' in the config file.")
 

    end_time = time.time()  # 记录结束时间
    execution_time = end_time - start_time  # 计算执行时间
    print(f"max_workers = {max_workers} executed in {execution_time:.4f} seconds.")

    # max_workers = 3 executed in 19.1932 seconds.
    # max_workers = 5 executed in 12.4322 seconds.
    # max_workers = 10 executed in 6.9659 seconds.
    # max_workers = 20 executed in 4.0377 seconds.
    # max_workers = 30 executed in 3.9492 seconds.
    # max_workers = 40 executed in 4.0305 seconds.


