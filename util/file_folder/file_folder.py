import re
import os

def get_latest_file_from_folder(folder_path:str
                                , file_pattern_regex:str=None
                                ):
    pattern = re.compile(file_pattern_regex)
    files = []
    for filename in os.listdir(folder_path):
        if file_pattern_regex:
            if pattern.match(filename):
                files.append(filename)
        else:
            files = os.listdir('/data/data/dev/ingestion/test/')
    file = max(files)
    return file