import download_police_bulletin
import PDF_text_counter
import lambda_scraper
import upload_counts_to_db
import asyncio
import PDF_address_counter


async def main(loop):
    # Download police bulletins locally
    # li = await download_police_bulletin.download_all(loop)

    # Process all local pdf files in ./pdfs, output a 'counts.csv' file
    #base_path = "./pdfs/"
    # PDF_text_counter.process_all_files(base_path, li)
    # PDF_text_counter.process_daily_counts(base_path)

    # Upload data from 'counts.csv' to DynamoDB
    # file_path = "./counts.csv"
    # upload_counts_to_db.upload_totals(file_path)

    # Upload data from 'daily_counts.json' to DynamoDB
    # file_path = "./daily_counts.json"
    # upload_counts_to_db.upload_daily_counts(file_path)

    # Run Lambda function
    lambda_scraper.main()

    # Process all local pdf files in ./pdfs, output a 'addresses.csv' file
    #PDF_address_counter.process_all_files(base_path)


if __name__ == '__main__':
    curr_loop = asyncio.get_event_loop()
    curr_loop.run_until_complete(main(curr_loop))
