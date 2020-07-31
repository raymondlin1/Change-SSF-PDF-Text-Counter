from os import path
from os import listdir
from PDF_text_counter import extract_text_from_pdf, extract_reason_for_arrest
import re
import boto3
import copy
from itertools import chain
from arcgis.gis import GIS
from arcgis.geocoding import geocode
import time
from PDF_text_counter import output_to_csv


my_gis = GIS()


# process all files from 'path' parameter and output to a file called 'addresses.csv'
def process_all_files(in_path):
    print("Starting address processing...")
    output_list = []
    start_time = time.time()
    temp_time = time.time()
    num_files = len(listdir(in_path))
    counter = 0
    for f in listdir(in_path):
        fd = open(path.join(in_path, f), 'rb')
        output_list +=  process_one_file(fd, f)

        curr_time = time.time()
        diff = curr_time - temp_time
        if diff >= 30:
            m, s = divmod(round(curr_time - start_time), 60)
            print("Elapsed for {} minutes and {} seconds. Processed {} out of {} files.".format(m, s, counter, num_files))
            print(output_list)
            print(len(output_list))
            temp_time = curr_time

        counter += 1

    print("Done processing all files. Outputting to csv...")
    output_to_csv(output_list, "addresses.csv")
    print("Finished!")


def process_one_file(fd, f):
    ret = []
    text = extract_text_from_pdf(fd)
    reasons = extract_reason_for_arrest(text)
    addresses = extract_address(text)
    new_date_str = get_date_from_file_name(f)
    #print(len(reasons))
    #print(len(addresses))
    if len(reasons) == len(addresses):
        for i in range(len(reasons)):
            curr = []
            curr.append(new_date_str)
            curr.append(reasons[i])
            curr.append(addresses[i])
            ret.append(curr)
    else:
        print("the addresses list and reasons list is not the same size")

    num_events = len(ret)
    for i in range(num_events):
        ret[i] += get_lat_long(ret[i][2])

    return ret


# gets the total number of counts in the 'reasons_counts' table
def get_reaons_counts():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('reasons_counts')
    res = table.scan()
    tot = 0
    for i in res["Items"]:
        tot += i['count']

    return tot


def extract_address(text):
    ret = []
    lines = text.split('\n')
    for i in range(len(lines)):
        if i == 0:
            continue
        tokens = re.split(" on | at |\. \.| , |, ", lines[i])
        if len(tokens) > 1:
            tokens = tokens[1:-1]
            n = len(tokens)
            cities = ["San Bruno", "So. San Francisco", "Daly City", "Brisbane", "Pacifica"]
            if tokens[n - 1] not in cities:
                tokens.append("So. San Francisco")

            tokens.append("CA, 94080")
            address = ", ".join(tokens)
            address = address.replace('/', " and ")

            ret.append(address)
    return ret


def send_api(queries):
    return


def get_date_from_file_name(f_name):
    tokens = f_name.split(".")
    date = tokens[0][-6:]
    month = date[0:2]
    day = date[2:4]
    year = "20{}".format(date[4:6])
    new_date_str = "{}-{}-{}".format(year, month, day)
    return new_date_str


def get_lat_long(single_line_address):
    res = geocode(single_line_address)[0]
    return [res['location']['y'], res['location']['x']]