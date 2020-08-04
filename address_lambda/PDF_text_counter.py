from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from os import path
from os import listdir
from io import StringIO
import re
import time
import csv
import json


def process_all_files(in_path, li):
    print("Started processing for all pdfs...")
    num_files = len(li)
    if num_files == 0:
        print("No new files to process. Finishing...")
        return
    d = {}
    count = 0
    start_time = time.time()
    temp_time = time.time()
    for f in li:
        count += 1
        fd = open(path.join(in_path, f), 'rb')
        text = extract_text_from_pdf(fd)
        reasons = extract_reason_for_arrest(text)
        count_occurrences(d, reasons)

        curr_time = time.time()
        diff = curr_time - temp_time
        if diff >= 30:
            m, s = divmod(round(curr_time - start_time), 60)
            print("Elapsed for {} minutes and {} seconds. "
                  "Currently processed {} of {} files.".format(m, s, count, num_files))
            temp_time = curr_time

    li = []
    for k in d.keys():
        li.append([k, d[k]])

    li.sort(reverse=True, key=lambda x: x[1])
    print("Outputting to csv file...")
    output_to_csv(li, 'counts.csv')
    print("Done!")


# processes all pdf files and outputs a json object with daily counts
def process_daily_counts(in_path):
    files_d = {}
    ret = []

    # check if daily_counts.json exists
    out_file = 'daily_counts.json'
    if path.exists(out_file):
        fd = open(out_file)
        li = json.load(fd)
        for l_item in li:
            files_d[l_item["date"]] = 1
            ret.append(l_item)

    count = 0
    for f_name in listdir(in_path):
        tokens = f_name.split('.')
        if tokens[1] == 'pdf':
            key = extract_date_from_title(f_name)
            if key not in files_d.keys():
                curr_d = {}
                fd = open(path.join(in_path, f_name), 'rb')
                text = extract_text_from_pdf(fd)
                reasons = extract_reason_for_arrest(text)
                count_occurrences(curr_d, reasons)

                # now use curr_d to generate a json obj and add it to a list
                curr_d["date"] = key
                ret.append(curr_d)

                count += 1
                print("processed {} files.".format(count))
            else:
                print("{} already processed, skipping".format(f_name))

    output_to_json(ret, out_file)


# given a list of 2-item lists that contains the reason for dispatch and the count,
# output the 2-item list to a csv file called counts.csv
def output_to_csv(li, f_name):
    with open(f_name, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(li)


def output_to_json(li, f_name):
    with open(f_name, 'w') as out_file:
        json.dump(li, out_file, indent=4)


# given a pdf file, extract the text
def extract_text_from_pdf(fd):
    rmgr = PDFResourceManager()
    retstr = StringIO()
    laparams = LAParams()
    device = TextConverter(rmgr, retstr, laparams=laparams)
    interpreter = PDFPageInterpreter(rmgr, device)
    password = ""
    maxpages = 0
    caching = True
    pagenos = set()

    for page in PDFPage.get_pages(fd, pagenos, maxpages=maxpages, password=password, caching=caching,
                                  check_extractable=True):
        interpreter.process_page(page)

    text = retstr.getvalue()
    fd.close()
    device.close()
    retstr.close()
    return text


# given text representing the media bulletin, extract the reasons for arrest and return a list
def extract_reason_for_arrest(text):
    lines = re.findall(r'[0-9][0-9]:[0-9][0-9] +.+ +[0-9]+', text)
    for i in range(len(lines)):
        tokens = re.split(r'\s{2,}', lines[i])
        lines[i] = tokens[1]

    return lines


# given a dictionary and a list of reasons for arrest, count all of the reasons and add to the dictionary
def count_occurrences(d, reasons):
    for r in reasons:
        if r in d.keys():
            d[r] += 1
        else:
            d[r] = 1


def extract_date_from_title(title):
    tokens = title.split('.')
    date = tokens[0][-6:]
    key = "20{}-{}-{}".format(date[-2:], date[0:2], date[2:4])
    return key
