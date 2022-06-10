from distutils.command.upload import upload
from classes.fsContainer import fsContainer
from classes.azContainer import azContainer, get_files
from utilities.confirm_upload import confirm_upload
from utilities.reporting import create_report
from config import load_config
from pathlib import Path
from misc.art import logo


# client secrects
config = load_config()
               
print(logo)
print("WARNING: All folders within the parent directory will be uploaded!!!")
while True:
    try:
        dir = Path(input("Enter a parent directory: "))
    except:
        print("Not a valid path")
        continue
    break

name = dir.name
uploads = azContainer(dir.name, config).upload_blobs(dir)

print(uploads["Summary"])
    
create_report(path=r'C:\Users\noel.vega\Projects\azStorage\reports\manifest.csv', rows=uploads["Records"], headers=["FilePath","bytes", "Container", "Success"])
create_report(path=r'C:\Users\noel.vega\Projects\azStorage\reports\summary.csv', rows=uploads["Summary"], headers=["Scope","Failed", "Complete", "GB", "Success_Rate"])

        
        
    

            
    
    
    
    
    
