from bs4 import BeautifulSoup
from os import path
from os import mkdir
import re
import asyncio
import aiohttp
import math

base_url = "http://ssf.net"


async def download_all(loop) -> None:
    print("Reading metadata...")
    d = read_metadata()

    urls = []

    # get urls from archives
    print("Gathering data from archives...")
    urls += await get_urls_from_page("https://www.ssf.net/departments/police/community/media-bulletins/-arch-1/", loop, d)

    # get urls from current
    print("Gathering data from current...")
    urls += await get_urls_from_page("https://www.ssf.net/departments/police/community/media-bulletins/", loop, d)

    print("Done pulling from pages. Starting to download each file...")

    async with aiohttp.ClientSession(loop=loop) as session:
        tasks = []
        for i in range(len(urls)):
            tasks.append(download_one_coro(urls[i], session))
            if i != 0 and i % 20 == 0:
                await asyncio.gather(*tasks)
                tasks = []
                print("At url {} now. Taking a break... ".format(i))
                await asyncio.sleep(5)

        if len(urls) == 0:
            print("No new files to download.")

        await asyncio.gather(*tasks)
        print("Finished. All pdfs should now be in ./pdfs")

async def get_urls_from_page(url, loop, d):
    ret = []
    async with aiohttp.ClientSession(loop=loop) as session:
        res = await session.get(url)
        content = await res.content.read()
        page = BeautifulSoup(content, 'html.parser')
        last_page = page.find('a', string=re.compile(".*Last.*"))['href']
        temp = ""
        i = len(last_page) - 1
        while last_page[i] != '-':
            temp = last_page[i] + temp
            i -= 1

        upper = int(temp) + 1

        for i in range(1, upper):
            to_search_url = "{}-npage-{}".format(url, i)
            if i == math.floor(upper / 2):
                print("Halfway done...")
            ret += await get_file_urls(to_search_url, loop, d)

        return ret


async def get_file_urls(url, loop, di):
    async with aiohttp.ClientSession(loop=loop) as session:
        res = await session.get(url)
        if res.status != 200:
            print("{} was a bad url. Responded with status {}.".format(url, res.status))
            return []

        content = await res.content.read()
        page = BeautifulSoup(content, 'html.parser')
        anchor_tags = page.findAll("a", {"class": "item-title"})
        ret = []
        for i in range(len(anchor_tags)):
            full_url = base_url + anchor_tags[i]["href"]
            if full_url not in di.keys():
                ret.append(full_url)

        return ret


def read_metadata():
    d = {}
    if path.exists("./pdfs/metadata.txt"):
        file = open("./pdfs/metadata.txt")
        lines = file.readlines()
        for li in lines:
            li = li[:-1]
            if li not in d.keys():
                d[li] = 1
    else:
        print("Metadata file doesn't exist. Skipping...")

    return d


async def download_one_coro(url, session) -> None:
    async with session.get(url) as res:
        if res.status != 200:
            return

        content = await res.content.read()
        page = BeautifulSoup(content, 'html.parser')
        anchor_tag = page.find(id=re.compile(".*- Police Media Bulletin"))
        if not anchor_tag:
            anchor_tag = page.find('a', string=re.compile(".* Media Bulletin"))
            if not anchor_tag:
                print("Police media bulletin not found in this page {}".format(url))
                return

        file_path = anchor_tag['href']
        full_file_url = base_url + file_path

        async with session.get(full_file_url) as res2:
            d = res2.headers['content-disposition']
            title = re.findall("filename=\"(.+)\"", d)[0]

            if path.exists("./pdfs/{}".format(title)):
                print("{} already exists. Skipping...".format(title))
                with open("./pdfs/metadata.txt", "a+") as f:
                    f.write(url)
                    f.write('\n')

                await res2.release()
                return

            pdf = await res2.content.read()
            local_path = "./pdfs/{}".format(title)

            if not path.exists("./pdfs"):
                print("Creating local directory 'pdfs'")
                mkdir("./pdfs")

            with open(local_path, "wb") as f:
                f.write(pdf)

            print("{} downloaded".format(title))
            if not path.exists("./pdfs/metadata.txt"):
                print("Creating metadata file 'metadata.txt'")

            with open("./pdfs/metadata.txt", "a+") as f:
                f.write(url)
                f.write('\n')

            await res2.release()

        await res.release()
