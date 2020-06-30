from bs4 import BeautifulSoup
from PDF_text_counter import extract_text_from_pdf, extract_reason_for_arrest, count_occurrences
from io import BytesIO
import boto3
import requests
import datetime
import re

url = "https://www.ssf.net/departments/police/community/media-bulletins/"
base_url = "http://ssf.net"


def parse_date(str):
    tokens = str.split('/')
    month = int(tokens[0])
    day = int(tokens[1])
    year = int(tokens[2])

    return datetime.date(year, month, day)


def is_yesterday(elem):
    yesterday_datetime = datetime.date.today() - datetime.timedelta(days=1)

    date_str = elem.find('p', {'class': 'item-date'}).string
    tokens = date_str.split()
    datetime_obj = parse_date(tokens[0])
    if (datetime_obj.year == yesterday_datetime.year and
            datetime_obj.month == yesterday_datetime.month and
            datetime_obj.day == yesterday_datetime.day):
        return True

    return False


def grab_new_bulletins():
    print("Grabbing new bulletins from SSF website...")
    res = requests.get(url)
    page = BeautifulSoup(res.content, 'html.parser')
    posted_dates = page.findAll('p', {'class': "item-date"})
    bulletins = []
    for p in posted_dates:
        if p.parent.name == 'li':
            bulletins.append(p.parent)

    bulletins = list(filter(is_yesterday, bulletins))
    print("Got all bulletins...")
    return bulletins


def process_new_bulletins(bulletins):
    if len(bulletins) == 0:
        print("No new bulletins to process...")
        return

    print("Processing new bulletins...")
    d = {}
    count = 0
    for b in bulletins:
        b_url = base_url + b.find('a', {'class': 'item-title'})['href']
        pdf = get_pdf(b_url)
        if pdf is not None:
            fd = BytesIO(pdf)
            text = extract_text_from_pdf(fd)
            reasons = extract_reason_for_arrest(text)
            count_occurrences(d, reasons)
        count += 1
        print("Currently processed {} of {} bulletins...".format(count, len(bulletins)))

    print("Updating the database...")
    for k in d.keys():
        # print("{}: {}".format(k, d[k]))
        curr_count = find_db(k)
        new_count = curr_count + d[k]
        update_db(k, new_count)

    print("Update complete.")


def get_pdf(in_url):
    res = requests.get(in_url)
    page = BeautifulSoup(res.content, 'html.parser')
    anchor_tag = page.find(id=re.compile(".*- Police Media Bulletin"))
    if not anchor_tag:
        anchor_tag = page.find('a', string=re.compile(".* Media Bulletin"))
        if not anchor_tag:
            print("Police media bulletin not found in this page {}".format(url))
            return None

    full_file_url = base_url + anchor_tag['href']
    res2 = requests.get(full_file_url)
    return res2.content


def update_db(key, value):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('reasons_counts')
    table.update_item(
        Key={'reason': key},
        UpdateExpression="SET #count=:c",
        ExpressionAttributeValues={
            ':c': value
        },
        ExpressionAttributeNames={
            '#count': "count"
        }
    )


def find_db(key):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('reasons_counts')
    res = table.get_item(Key={'reason': key})["Item"]
    return res["count"]


def main(event=None, lambda_context=None):
    new_bulletins = grab_new_bulletins()
    process_new_bulletins(new_bulletins)


if __name__ == '__main__':
    main()
