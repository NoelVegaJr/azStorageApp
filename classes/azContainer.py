from http import client
from azure.storage.blob import ContainerClient
from datetime import datetime
from pathlib import Path
import os
import csv

def az_upload(client, filepath):
    file = Path(filepath)    
    container_name = client.get_container_properties()["name"]
    file_size_bytes = os.path.getsize(file)
    with open(file, "rb") as data:
        try:
            client.get_blob_client(file.name).upload_blob(data)
            print("successful upload")
            return {"FilePath": file, "bytes": file_size_bytes,"Container": container_name, "Success": True}
        except:
            print("failed upload")
            return {"FilePath": file, "bytes": file_size_bytes,"Container": container_name, "Success": False}


def get_files(dir, recurse=False):
    if recurse:
        return [item for item in Path(dir).glob('**/*') if item.is_file()]
    else:
        return [item for item in Path(dir).iterdir() if item.is_file()]
    


class azContainer:
    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.client = ContainerClient.from_connection_string(self.config['azure_storage_connection'], name)
        
    def create(self):
        if not self.client.exists():
            self.client.create_container()
        else:
            print("Already exists.")
            
    def upload_blobs(self, path):
        data = {
            "Records": [],
            "Summary": {
                "Scope": 0, 
                "Failed": 0, 
                "Complete": 0, 
                "GB": 0, 
                "Success_Rate": 0
                }
         }
        
        self.create()
        if Path(path).is_dir():  
            files = [item for item in Path(path).iterdir() if item.is_file()]
            
        else:
            files = [Path(path)]
        
        data["Summary"]["Scope"] = len(files)
        
        for file in files:
            response = az_upload(self.client, file)
            data["Records"].append(response)
            data["Summary"]["GB"] += response["bytes"] / 1024**3
            if response["Success"]:
                data["Summary"]["Complete"] += 1
            else:
                data["Summary"]["Failed"] += 1
                
        data["Summary"]["Success_Rate"] = float(data["Summary"]["Complete"]) / data["Summary"]["Scope"]

        return data
    
    def download_blobs(self, name):
        return self.client.download_blob(name)
    
    def get_blobs(self):
        blobs = {}
        for blob in self.client.list_blobs():
            blobs[blob["name"]] = {"ContainerName":blob["container"], "bytes":blob["size"]}
        return blobs

    def create_manifest(self):
        now = datetime.now().strftime("%m-%d-%y %H:%M:%S")
        dir_root = os.path.dirname(os.path.abspath(__file__)) + fr"\manifests\{self.name}"
        if not os.path.exists(dir_root):
            os.makedirs(dir_root)
            
        with open(fr"{dir_root}\az-{self.name}.csv", "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=',', fieldnames=['ContainerName', 'BlobName', 'bytes'])
            writer.writeheader()
            writer.writerows(self.get_blobs())
        
        return fr"{dir_root}\az-{self.name}.csv"
    