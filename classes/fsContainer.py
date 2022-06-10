import os
import csv
from pathlib import Path
from azure.storage.blob import ContainerClient

['FullName', 'ContainerName', 'FileName', 'bytes']



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



   
