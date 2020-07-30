from os import path
from os import listdir
from PDF_text_counter import extract_text_from_pdf
import re
from itertools import chain


# process all files from 'path' parameter and output to a file called 'addresses.csv'
def process_all_files(in_path):
    count = 0
    for f in listdir(in_path):
        fd = open(path.join(in_path, f), 'rb')
        process_one_file(fd, f)
        #break # remove later
        if count == 1:
            break
        count += 1


def process_one_file(fd, f):
    ret = []
    text = extract_text_from_pdf(fd)
    lines = text.split('\n')
    for li in lines:
        tokens = re.split(" on | at |\. \.", li)
        if len(tokens) > 1:
            location = tokens[1:-1]
            if len(location) == 1:
                location = [None] + location

            location[1] = location[1].split(', ')
            #location = list(chain(*location))
            print(location)
    print(f)