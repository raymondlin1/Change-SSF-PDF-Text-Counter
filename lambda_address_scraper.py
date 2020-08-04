from lambda_scraper import grab_new_bulletins, update_timestamp_db, get_pdf, extract_text_from_pdf, extract_reason_for_arrest
from io import BytesIO
from helper import get_date_from_file_name, extract_address
import boto3
import base64
from botocore.exceptions import ClientError
import psycopg2
import psycopg2.extras
import json
import requests
import os


base_url = "http://ssf.net"
secret = None


def get_secret():
    global secret
    secret_name = "changessf-db-credentials"
    region_name = "us-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            secret = decoded_binary_secret


def process_new_bulletins(bulletins):
    global secret
    if len(bulletins) == 0:
        print("No new bulletins to process...")
        print("Updating timestamp in dynamodb")
        update_timestamp_db()
        return

    data = []

    print("Processing new bulletins...")
    for b in bulletins:
        b_url = base_url + b.find('a', {'class': 'item-title'})['href']
        temp = get_pdf(b_url)
        pdf = temp[0]
        title = temp[1]

        if pdf is not None:
            print("Processing {}...".format(title))
            fd = BytesIO(pdf)
            entries = process_one_file(fd, title)
            for i in range(len(entries)):
                # sanitize the input to replace one single quotes with two single quotes
                for j in range(len(entries[i])):
                    if isinstance(entries[i][j], str):
                        entries[i][j] = entries[i][j].replace("'", "''")

                # if the last two values in entries[i] are not None, then we found a lat/long for it
                if entries[i][3] is not None and entries[i][4] is not None:
                    data.append((entries[i][0], entries[i][1], entries[i][2], entries[i][3], entries[i][4]))

            print("Done processing {}...".format(title))

    print("Inserting into RDS db...")
    print("Getting RDS db credenitals...")
    get_secret()
    print("Got them!")

    print("Connecting to RDS database...")
    secret = json.loads(secret)

    conn = psycopg2.connect(
        database=secret["engine"],
        user=secret["username"],
        password=secret["password"],
        host=secret["host"],
        port=secret["port"])

    print("Connected!")

    try:
        cur = conn.cursor()
        sql = "INSERT INTO locations (date, reason, address, latitude, longitude) VALUES %s ON CONFLICT (date, reason, address) DO NOTHING;"
        print(sql)
        print(data)
        psycopg2.extras.execute_values(cur, sql, data)
        cur.close()
    except Exception as error:
        conn.rollback()
        print(error)
        return
    finally:
        if conn is not None:
            conn.commit()
            conn.close()
            print("Successfully inserted into RDS...")

    update_timestamp_db()
    print("Finished.")


def process_one_file(fd, f):
    ret = []
    text = extract_text_from_pdf(fd)
    reasons = extract_reason_for_arrest(text)
    addresses = extract_address(text)
    new_date_str = get_date_from_file_name(f)

    if len(reasons) == len(addresses):
        for i in range(len(reasons)):
            curr = [new_date_str, reasons[i], addresses[i]]
            ret.append(curr)
    else:
        print("the addresses list and reasons list is not the same size - file {} - reasons length: {}, addresses length: {}".format(f, len(reasons), len(addresses)))
        return ret

    num_events = len(ret)
    for i in range(num_events):
        res = requests.get("https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}".format(ret[i][2], os.environ["GOOGLE_API_KEY"]))
        d = res.json()
        if 'results' in d and len(d["results"]) > 0 and 'geometry' in d["results"][0] and "location" in d["results"][0]["geometry"]:
            lat_long = d["results"][0]["geometry"]["location"]
            ret[i].append(lat_long["lat"])
            ret[i].append(lat_long["lng"])
        else:
            ret[i] += [None, None]

    return ret


def main(event=None, lambda_context=None):
    new_bulletins = grab_new_bulletins()
    process_new_bulletins(new_bulletins)


if __name__ == '__main__':
    main()