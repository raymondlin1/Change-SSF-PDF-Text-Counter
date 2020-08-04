# Change-SSF-PDF-Text-Counter

This is one of the backend repos for ChangeSSF data visualization web app. Here are the links to the other parts of the project:
* [Backend API](https://github.com/raymondlin1/Change-SSF-API/tree/master)
* [Frontend](https://github.com/raymondlin1/Change-SSF-Web-App)
* [Live Demo](http://changessfwebapp2-env.eba-cpejdgpp.us-west-1.elasticbeanstalk.com)

This part of the project was developed using Python. 

## Scripts

Here is a description of each script and its purpose:

[lambda_scraper.py](https://github.com/raymondlin1/Change-SSF-PDF-Text-Counter/blob/master/lambda_scraper.py): lambda function that downloads and extracts reasons from media bulletins from 
[this website](https://www.ssf.net/departments/police/community/media-bulletins/-npage-49), as they are uploaded periodically.  

[lambda_address_scraper.py](https://github.com/raymondlin1/Change-SSF-PDF-Text-Counter/blob/master/lambda_address_scraper.py): lambda function that does the same thing as above, but instead 
of extracting reasons, it extracts the address of the dispatch, sends an API called to Google Places to retrieve the address latitude and longitude

[download_police_bulletin.py](https://github.com/raymondlin1/Change-SSF-PDF-Text-Counter/blob/master/download_police_bulletin.py): script that downloads all media bulletins from 
[this website](https://www.ssf.net/departments/police/community/media-bulletins/-npage-49) into a folder called 'pdf', able to stop the script at any time and continue from where it left off.

[PDF_text_counter.py](https://github.com/raymondlin1/Change-SSF-PDF-Text-Counter/blob/master/PDF_text_counter.py): script that processes all the pdf files in the directory './pdfs/', and 
and produces a count of each reason for dispatch into a filed called 'counts.csv'.

[PDF_address_counter.py](https://github.com/raymondlin1/Change-SSF-PDF-Text-Counter/blob/master/PDF_address_counter.py): script that does the same thing as above, but instead of outputting 
the counts, it outputs the addresses and their latitudes and longitudes.

[helper.py](https://github.com/raymondlin1/Change-SSF-PDF-Text-Counter/blob/master/helper.py): file containing helper functions utilized by multiple files

[main.py](https://github.com/raymondlin1/Change-SSF-PDF-Text-Counter/blob/master/main.py): driver code

[addresses.csv](https://github.com/raymondlin1/Change-SSF-PDF-Text-Counter/blob/master/addresses.csv): output of PDF_address_counter.py

[counts.csv](https://github.com/raymondlin1/Change-SSF-PDF-Text-Counter/blob/master/counts.csv): output of PDF_text_counter.py

[upload_counts_to_db.py](https://github.com/raymondlin1/Change-SSF-PDF-Text-Counter/blob/master/upload_counts_to_db.py): script used to upload 'counts.csv' and 'addresses.csv' to the appropriate databases.
