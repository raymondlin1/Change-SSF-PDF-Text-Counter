import os
import csv
import boto3
import json
import time
import psycopg2


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


def upload_addresses(file_path):
    if not os.path.exists(file_path):
        print("Error: file not found...")
        return

    print("Starting address uploading...")
    conn = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="<db_password_here>",
        host="database-1.cjqv1oqppg0b.us-west-1.rds.amazonaws.com",
        port='5432'
    )

    start_time = time.time()
    temp_time = time.time()
    counter = 0
    values = ""
    with open(file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if len(row) >= 4:
                for i in range(len(row)):
                    row[i] = row[i].replace("'", "''")
                values += "(\'{}\', \'{}\', \'{}\', {}, {}),\n".format(row[0], row[1], row[2], row[3], row[4])

            counter += 1
            curr_time = time.time()
            diff = curr_time - temp_time
            if diff >= 30:
                m, s = divmod(round(curr_time - start_time), 60)
                print("Elapsed for {} minutes and {} seconds. Processed {} lines".format(m, s, counter))
                temp_time = curr_time
    print("Done reading csv file...")
    try:
        curr = conn.cursor()
        curr.execute("INSERT INTO locations (date, reason, address, latitude, longitude) VALUES {} ON CONFLICT (date, reason, address) DO NOTHING;".format(values[:-2]))
        curr.close()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.commit()
            print("committing changes...")
            conn.close()

    print("Done")
