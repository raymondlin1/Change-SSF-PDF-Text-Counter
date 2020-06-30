import download_police_bulletin
import PDF_text_counter
import lambda_scraper
import upload_counts_to_db
import asyncio


async def main(loop):
    # Download police bulletins locally
    li = await download_police_bulletin.download_all(loop)

    # Process all local pdf files in ./pdfs, output a 'counts.csv' file
    base_path = "./pdfs/"
    PDF_text_counter.process_all_files(base_path, li)

    # Upload data from 'counts.csv' to DynamoDB
    # file_path = "./counts.csv"
    # upload_counts_to_db.upload(file_path)

    # Run Lambda function
    # lambda_scraper.main()


if __name__ == '__main__':
    curr_loop = asyncio.get_event_loop()
    curr_loop.run_until_complete(main(curr_loop))
