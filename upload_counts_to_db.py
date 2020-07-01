import os
import csv
import boto3
import json


def upload_totals(file_path):
    if not os.path.exists(file_path):
        print("Error: file not found...")
        return

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('reasons_counts')
    with open(file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            res = table.put_item(
                Item={
                    'reason': row[0],
                    'count': int(row[1])
                }
            )
            print(res)


def upload_daily_counts(file_path):
    if not os.path.exists(file_path):
        print("Error: file not found...")
        return

    fd = open(file_path)
    date_list = json.load(fd)
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('daily_reason_counts')

    for d in date_list:
        res = table.put_item(Item=d)
        print(res)
