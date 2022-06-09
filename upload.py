import os
import yaml
from progressbar import progress_bar
import csv
import shutil
from azure.storage.blob import ContainerClient
import time
from pathlib import Path
import logging

logging.basicConfig(level=logging.DEBUG, filename="log.log", filemode="w",
                    format="%(asctime)s - %(levelname)s - %(message)s")

logger = logging.getLogger("__name__")


def load_config():
    dir_root = os.path.dirname(os.path.abspath(__file__))
    with open(dir_root + "/config.yaml", "r") as yamlfile:
        return yaml.load(yamlfile, Loader=yaml.FullLoader)




class fsContainer:
    def __init__(self, path, config):
        self.path = path
        self.name = path.name
        self.config = config
        self.client = ContainerClient.from_connection_string(self.config['azure_storage_connection'], self.name)
    
    def get_files(self):
        with os.scandir(self.path) as entries:
            for entry in entries:
                if entry.is_file() and not entry.name.startswith('.'):
                    yield {"FullName": entry.path, "ContainerName": self.name, "FileName": entry.name, "bytes": os.path.getsize(entry.path)}
    
    def create_manifest(self):
        dir_root = os.path.dirname(os.path.abspath(__file__)) + fr"\manifests\{self.name}"
        if not os.path.exists(dir_root):
            os.makedirs(dir_root)
            
        with open(fr"{dir_root}\fs-{self.name}.csv", "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=',', fieldnames=['FullName', 'ContainerName', 'FileName', 'bytes'])
            writer.writeheader()
            writer.writerows(self.get_files())
        return fr"{dir_root}\fs-{self.name}.csv"


class azContainer:
    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.client = ContainerClient.from_connection_string(self.config['azure_storage_connection'], name)
        
    def create_azcontainer(self):
        if not self.client.exists():
            self.client.create_container()
        else:
            print("Already exists.")
    
    def upload(self, files): 
        print("Uploading files to blob storage...\n")
        for file in files:
            blob_client = self.client.get_blob_client(file["FileName"])
            with open(file["FullName"], "rb") as data:
                blob_client.upload_blob(data)
   
    def get_blobs(self):
        for item in self.client.list_blobs():
            yield {"ContainerName":item["container"], "BlobName": item["name"], "bytes":item["size"]}

    def create_manifest(self):
        dir_root = os.path.dirname(os.path.abspath(__file__)) + fr"\manifests\{self.name}"
        if not os.path.exists(dir_root):
            os.makedirs(dir_root)
            
        with open(fr"{dir_root}\az-{self.name}.csv", "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=',', fieldnames=['ContainerName', 'BlobName', 'bytes'])
            writer.writeheader()
            writer.writerows(self.get_blobs())
        
        return fr"{dir_root}\az-{self.name}.csv"
    


   
