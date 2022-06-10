import csv
from pathlib import Path
import shutil

def confirm_upload(fs_container, az_container, config):
    fs_manifest = fs_container.create_manifest()
    az_manifest = az_container.create_manifest()
    files = {}
    with open(fs_manifest, newline='') as file:
        reader = csv.DictReader(file, delimiter=",")
        for row in reader:
            files[row["FileName"]] = int(row["bytes"])
    
    blobs = {}
    with open(az_manifest, newline='') as file:
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
