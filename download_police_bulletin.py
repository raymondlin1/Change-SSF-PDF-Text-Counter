from bs4 import BeautifulSoup
from os import path
import requests
import re

def func() -> None:
    download_one("https://www.ssf.net/Home/Components/News/News/397/804?arch=1&npage=49")


def download_one(url) -> None:
    res = requests.get(url)
    if res.status_code != 200:
        print("{} was a bad url. Responded with status code {}.".format(url, res.status_code))
        return

    page = BeautifulSoup(res.content, 'html.parser')
    regex = re.compile(".*- Police Media Bulletin")
    anchor_tag = page.find(id=regex)
    if not anchor_tag:
        print("{} is not a police bulletin file. Skipping...".format(url))
        return

    file_path = anchor_tag['href']
    full_file_url = "http://ssf.net" + file_path

    res2 = requests.get(full_file_url)
    d = res2.headers['content-disposition']
    title = re.findall("filename=\"(.+)\"", d)[0]

    if path.exists("./pdfs/{}".format(title)):
        print("{} already exists. Skipping...".format(title))
        return

    pdf = res2.content
    local_path = "./pdfs/{}".format(title)
    with open(local_path, "wb") as f:
        f.write(pdf)

    return
