from upload import fsContainer, azContainer, load_config
from pathlib import Path
import shutil
import csv
from art import logo

def confirm_upload(fs_container_manifest, az_container_manifest):
    files = {}
    with open(fs_container_manifest, newline='') as file:
        reader = csv.DictReader(file, delimiter=",")
        for row in reader:
            files[row["FileName"]] = int(row["bytes"])
    
    blobs = {}
    with open(az_container_manifest, newline='') as file:
        reader = csv.DictReader(file, delimiter=",")
        for row in reader:
            blobs[row["BlobName"]] = int(row["bytes"])
    
    if len(files) == len(blobs) and sum(files.values()) == sum(blobs.values()):
        archive = Path(config["archive_dir"])
        if not archive.exists():
            archive.mkdir()
            
        if not (archive / fs_container.name).exists():
            shutil.move(fs_container.path, archive)
    else:
        archive = Path(config["faild_dir"])
        if not archive.exists():
            archive.mkdir()
        
        if not (archive / fs_container.name).exists():
            shutil.move(fs_container.path, archive)

# client secrects
config = load_config()
               
print(logo)
print("WARNING: All folders within the parent directory will be uploaded!!!")
parent_directory = input("Enter a parent directory: ")
for fs_container in [fsContainer(item, config) for item in Path(parent_directory).iterdir() if item.is_dir() and 'container' in item.name]:
    az_container = azContainer(fs_container.name, config)
    az_container.create_azcontainer()
    az_container.upload(fs_container.get_files())
    fs_manifest = fs_container.create_manifest()
    az_manifest = az_container.create_manifest()
    confirm_upload(fs_manifest, az_manifest)


        
        
    

            
    
    
    
    
    
