from pathlib import Path
import os, csv

def create_report(path, rows, headers):
    path = Path(path)
    dir = path.parent

    if not os.path.exists(dir):
        os.makedirs(dir)
        
    with open(path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, delimiter=',', fieldnames=headers)
        writer.writeheader()
        if len(rows) == 1:
            writer.writerow(rows)
        else:
            writer.writerows(rows)
    return path