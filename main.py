# -*- encoding: utf-8 -*-
'''
@Time    :   2024/11/18 16:15:16
@Author  :   Li Zeng 
'''


# pip install paramiko
import paramiko
import os
import configparser
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
    
    return hostname, port, username, password, remote_folder, local_folder


def download_folder_via_sftp(hostname, port, username, password, remote_folder, local_folder):
    # 创建SFTP客户端
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, port, username, password)

    sftp = ssh.open_sftp()

    # 确保本地文件夹存在
    if not os.path.exists(local_folder):
        os.makedirs(local_folder)

    # 遍历远程文件夹中的文件并下载
    remote_file_list = sftp.listdir(remote_folder)
    print(remote_file_list)
    for file_name in remote_file_list:
        if ".py" in file_name:
            remote_file_path = os.path.join(remote_folder, file_name)
            local_file_path = os.path.join(local_folder, file_name)
            print(remote_file_path, local_file_path)
            sftp.get(remote_file_path, local_file_path)

    # 关闭SFTP和SSH连接
    sftp.close()
    ssh.close()


def upload_folder_via_sftp(hostname, port, username, password, remote_folder, local_folder):
    # 创建SFTP客户端
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, port, username, password)
 
    sftp = ssh.open_sftp()
 
    # 确保远程文件夹存在
    try:
        sftp.listdir(remote_folder)
    except IOError:
        # 如果远程文件夹不存在，则创建它
        sftp.mkdir(remote_folder)
 
    # 遍历本地文件夹中的文件并上传
    for file_name in os.listdir(local_folder):
        if file_name.endswith(".py"):  # 假设我们只上传 .py 文件
            local_file_path = os.path.join(local_folder, file_name)
            remote_file_path = os.path.join(remote_folder, file_name)
            print(local_file_path, remote_file_path)
            sftp.put(local_file_path, remote_file_path)
 
    # 关闭SFTP和SSH连接
    sftp.close()
    ssh.close()


if __name__ == '__main__':
    # 读取配置文件中的参数
    config_file = './config.ini'
    print(config_file)
    hostname, port, username, password, remote_folder, local_folder, max_workers, operation_mode = read_config(config_file)

    start_time = time.time()  # 记录开始时间

    if operation_mode == 1:
        # 调用上传函数
        upload_folder_via_sftp(hostname, port, username, password, remote_folder, local_folder)
    elif operation_mode == 2:
        # 调用下载函数
        download_folder_via_sftp(hostname, port, username, password, remote_folder, local_folder)
    else:
        print("Invalid operation mode. Please set 'operation_mode' to either 'upload' or 'download' in the config file.")

    end_time = time.time()  # 记录结束时间
    execution_time = end_time - start_time  # 计算执行时间
    print(f"executed in {execution_time:.4f} seconds.")

    # executed in 10.0362 seconds. upload
    # executed in 11.4640 seconds. download