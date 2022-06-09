import random
import os
from pathlib import Path
from progressbar import progress_bar
NUMBER_OF_FILES = 400000
MAX_CHAR_LENGTH = 2

for container in range(1):
    dir_root = os.path.dirname(os.path.abspath(__file__)) + fr"\big_container\container-{container}"
    print(dir_root)
    if not os.path.exists(dir_root):
        os.makedirs(dir_root)
        
    progress_bar(0, NUMBER_OF_FILES)
    for x in range(NUMBER_OF_FILES):
        with open(f"{dir_root}\{x}.txt", "w") as file:
            numbers = random.randrange(1,MAX_CHAR_LENGTH)
            for number in range(numbers):
                file.write(str(round(number)))
        progress_bar(x + 1, NUMBER_OF_FILES)
        
                
