from bs4 import BeautifulSoup
from PDF_text_counter import extract_text_from_pdf, extract_reason_for_arrest, count_occurrences, extract_date_from_title
from io import BytesIO
import boto3
import requests
import datetime
import re
import pytz

url = "https://www.ssf.net/departments/police/community/media-bulletins/"
base_url = "http://ssf.net"


def parse_date(str):
    tokens = str.split('/')
    month = int(tokens[0])
    day = int(tokens[1])
    year = int(tokens[2])

    return datetime.date(year, month, day)


def is_yesterday(elem):
    yesterday_utc_datetime = pytz.utc.localize(datetime.datetime.utcnow() - datetime.timedelta(days=1))
    yesterday_pst_datetime = yesterday_utc_datetime.astimezone(pytz.timezone("America/Los_Angeles"))

    # Dummy variable, remove later
    # yesterday_pst_datetime = datetime.datetime(2020, 6, 23)

    date_str = elem.find('p', {'class': 'item-date'}).string
    tokens = date_str.split()
    datetime_obj = parse_date(tokens[0])
    if (datetime_obj.year == yesterday_pst_datetime.year and
            datetime_obj.month == yesterday_pst_datetime.month and
            datetime_obj.day == yesterday_pst_datetime.day):
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
        update_timestamp_db()
        return

    print("Processing new bulletins...")
    d = {}
    count = 0

    for b in bulletins:
        b_url = base_url + b.find('a', {'class': 'item-title'})['href']
        temp = get_pdf(b_url)
        pdf = temp[0]
        title = temp[1]
        date = extract_date_from_title(title)
        curr_d = {}
        if pdf is not None:
            fd = BytesIO(pdf)
            text = extract_text_from_pdf(fd)
            reasons = extract_reason_for_arrest(text)
            count_occurrences(curr_d, reasons)

        for k in curr_d.keys():
            if k in d.keys():
                d[k] += curr_d[k]
            else:
                d[k] = curr_d[k]

        curr_d["date"] = date
        # updates the daily_reasons_counts table
        update_db_daily_counts(curr_d)

        count += 1
        print("Currently processed {} of {} bulletins...".format(count, len(bulletins)))

    print("Updating the reasons_counts in db...")
    for k in d.keys():
        # print("{}: {}".format(k, d[k]))
        curr_count = find_db(k)
        new_count = curr_count + d[k]

        # updates the 'reasons_counts' table
        update_db_reasons_counts(k, new_count)

    # updates the 'last_updated' timestamp
    update_timestamp_db()
    print("Update complete.")


# given the url to the pdf download page, return the downloaded pdf and its title
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
    title = res2.headers['content-disposition']
    return res2.content, title


def update_db_reasons_counts(key, value):
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


def update_db_daily_counts(injson):
    print("Putting into daily_counts, key {}".format(injson["date"]))
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('daily_reason_counts')
    res = table.put_item(
        Item=injson
    )
    print("Status code: {}".format(res['ResponseMetadata']['HTTPStatusCode']))


def find_db(key):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('reasons_counts')

    res = table.get_item(Key={'reason': key})
    if 'Item' not in res.keys():
        # print(res)
        return 0

    return res["Item"]["count"]


def update_timestamp_db():
    print("Updating timestamp in db...")
    tz = pytz.timezone('America/Los_Angeles')
    curr_timestamp = round(datetime.datetime.now(tz).timestamp())

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('last_updated')
    res = table.update_item(
        Key={'key': 'last_updated_time'},
        UpdateExpression="SET #t=:t",
        ExpressionAttributeValues={
            ':t': curr_timestamp
        },
        ExpressionAttributeNames={
            '#t': "timestamp"
        }
    )
    status_code = res['ResponseMetadata']['HTTPStatusCode']
    if status_code != 200:
        print("Error updating the db: status code {}.".format(status_code))
    else:
        print("Successfully updated timestamp in db.")


def main(event=None, lambda_context=None):
    new_bulletins = grab_new_bulletins()
    process_new_bulletins(new_bulletins)


if __name__ == '__main__':
    main()
