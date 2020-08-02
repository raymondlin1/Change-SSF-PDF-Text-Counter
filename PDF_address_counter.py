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
import csv


my_gis = GIS()


# process all files from 'path' parameter and output to a file called 'addresses.csv'
def process_all_files(in_path):
    print("Starting address processing...")
    d = read_metadata()
    output_list = []
    start_time = time.time()
    temp_time = time.time()
    num_files = len(listdir(in_path))
    counter = len(d.keys())

    with open("address_metadata.txt", "a") as address_metadata_file:
        for f in listdir(in_path):
            if f not in d.keys():
                fd = open(path.join(in_path, f), 'rb')
                output_list += process_one_file(fd, f)

                curr_time = time.time()
                diff = curr_time - temp_time
                if diff >= 30:
                    m, s = divmod(round(curr_time - start_time), 60)
                    print("Elapsed for {} minutes and {} seconds. Processed {} out of {} files.".format(m, s, counter, num_files))
                    temp_time = curr_time

                output_to_csv_append(output_list, "addresses.csv")
                output_list = []
                address_metadata_file.write("{}\n".format(f))
                address_metadata_file.flush()
                counter += 1
            else:
                print("File {} already processed - skipping...".format(f))

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
        # if lat long is the same as (37.653995000000066,-122.41306999999995), then skip it, since it
        # means it wasn't able to find the address
        lat_long = get_lat_long(ret[i][2])
        if lat_long[0] != 37.653995000000066 or lat_long[1] != -122.41306999999995:
            ret[i] += lat_long

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


def output_to_csv_append(li, f_name):
    with open(f_name, 'a', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(li)


# read 'address_metadata.txt' to see which files we have processed, returns a dictionary
def read_metadata():
    d = {}
    if path.exists("./address_metadata.txt"):
        print("Address metadata file exists, going to read it...")
        file = open("./address_metadata.txt")
        lines = file.readlines()
        for li in lines:
            # remove the \n character
            li = li[:-1]
            if li not in d.keys():
                d[li] = 1
    else:
        print("Address metadata file not found - skipping...")

    return d

