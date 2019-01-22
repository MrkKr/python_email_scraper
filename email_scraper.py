#!/usr/bin/python3
from bs4 import BeautifulSoup
import requests
import requests.exceptions
from urllib.parse import urlsplit
from collections import deque
import re
import csv
import fileinput

# a queue of urls to be crawled
new_urls = deque()

# a queue URLs from CSV
csv_urls = deque()

# open CSV file, URL from each separate line are added to csv_urls
with open('', newline='') as r:
    reader = csv.reader(r)
    for row in reader:
        csv_urls.extend(row)

# loop from links
while len(csv_urls):
    print('csv_urls')
    print(csv_urls)
    url_csv = csv_urls.popleft()

    # add separated URL to be crawled from CSV to new_urls pojedynczy URL z CSV do new_urls
    new_urls.append(url_csv)

    # a set of urls that we have already crawled
    processed_urls = set()

    # a set of crawled emails
    emails = set()

    # process URLs one by one until we exhaust the queue
    while len(new_urls):
        print('new_urls')
        print(new_urls)

        # move next URL from the queue to the set of processed URLs
        url = new_urls.popleft()
        processed_urls.add(url)

        # extract base url to resolve relative links
        parts = urlsplit(url)
        base_url = "{0.scheme}://{0.netloc}".format(parts)
        path = url[:url.rfind('/')+1] if '/' in parts.path else url

        # get url's content
        print("Processing %s" % url)
        try:
            response = requests.get(url, allow_redirects=True)
        except (requests.exceptions.MissingSchema, requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout,
                requests.exceptions.TooManyRedirects, requests.exceptions.InvalidURL, requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ContentDecodingError, requests.exceptions.HTTPError):
            # ignore pages with errors above
            continue

        #save email result to CSV
        with open('results.csv', "a") as csvfile:
            # extract all email addresses and add them into the resulting set
            new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", response.text, re.I))
            print (emails)
            print(len(emails))
            print (new_emails)
            emails.update(new_emails)
            spamwriter = csv.writer(csvfile, delimiter=" ",
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for item in new_emails:
                spamwriter.writerows([item.split(' ')])

        # create a beautiful soup for the html document
        soup = BeautifulSoup(response.text, 'html.parser')

        # find and process all the anchors in the document
        for anchor in soup.find_all("a"):
            # extract link url from the anchor
            link = anchor.attrs["href"] if "href" in anchor.attrs else ''
            # resolve relative link
            if link.startswith('/'):
                link = base_url + link
            elif not link.startswith('http'):
                link = path + link
            # skip link if is not come from the same domain
            elif link.startswith(path) == False:
                continue
            # skip link with extensions below
            if link.endswith( ('.jpg', '.JPG', '.jpeg', '.ico', '.png', '.gif', '.eps', '.pdf', '.css', '.csv', '.js', '.zip', '.rar', '.mp4', '.mp3', '.ogg', '.mov', '.m4v', '.webm', '.ogv', '.swf', 'void(0);', '.xls', '.xlsx', '.doc', '.docx', '#', '#0', 'cart') ) == True:
                continue
            # skip link if HAS one of the following words:
            prefixy_1 = ('javascript:', 'blog','Blog', 'category', 'calendar', 'facebook', 'twitter', 'linkedin', 'instagram', 'tel:+', 'tel:', 'sitemap', 'site-map', 'mailto', 'privacy-policy', 'products', 'news')
            if any(s in link for s in prefixy_1):
                print('znaleziono zbędne prefixy w URLu: ' + link)
                continue
            # skip link if HAS NOT one of the following words:
            prefixy_2 = ('contact', 'Contact', 'contacts', 'Contacs', 'kontakt', 'Kontakt', 'about-us', 'About-us', 'About-Us', 'aboutus', 'Aboutus', 'about', 'About', 'findus', 'find-us', 'Findus', 'Find-us', 'Find-Us', 'contact-us', 'Contact-Us','Contact-us', 'contactus', 'Contactus', 'meetus','meet-us')
            if not any(s in link for s in prefixy_2):
                continue

            print('Link: ' + link)

            # add the new url to the queue if it was not enqueued nor processed yet
            if not link in new_urls and not link in processed_urls:
                new_urls.append(link)

# check and remove duplicated URLs in CSV result file
seen = set() # set for fast O(1) amortized lookup
for line in fileinput.FileInput('results.csv', inplace=1):
    if line not in seen:
        seen.add(line)
        print (line.strip('\n')), # standard output is now redirected to the file
print(seen)