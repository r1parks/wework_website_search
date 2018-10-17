#!/usr/bin/env python

"""
Fetches urls from the given url list, searches the associated web pages for
words that start and end with 's' and returns the three most common words
found along with their frequency count (tried to make it interesting)
"""

from Queue import Queue
from collections import Counter
from threading import Thread
import requests
import logging

# url to fetch website list
WEBSITE_LIST_URL = 'https://s3.amazonaws.com/fieldlens-public/urls.txt'

# our target regex to search websites for
# in this case we're searching for words that start and end with s and contain at least one vowel
SEARCH_REGEX = r'\bs[a-z]*[aeiou][a-z]*s\b'

# Number of worker threads for fetching and processing websites
NTHREADS = 20

# Where to write our output
OUTPUT_FILE_NAME = 'results.txt'

# Uncomment this to use the google chrome user agent
# Warning that this will violate a lot of terms of servcie so this should never be done
# because that would be against the rules
REQUEST_HEADERS = {
    # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
}

# set the log level to INFO to get all of our scraping output
logging.basicConfig(level=logging.INFO)


def website_list():
    """ fetch the website list and parse the urls from it """
    response = requests.get(WEBSITE_LIST_URL)
    lines = response.text.split('\n')[1:]
    return ['https://{}'.format(line.split(',')[1][1:-1]) for line in lines if ',' in line]


def search_site(url):
    """
    Fetches and the provided url and searches the result. In this case the result will be
    the most common three words which match the SEARCH_REGEX with their associated counts.

    In the case of an error an error string is returned and an error is logged.
    """
    import re
    try:
        # timeout is set to 3000ms, otherwise bad requests will hang for quite a while tying up the thread
        response = requests.get(url, timeout=3.0, headers=REQUEST_HEADERS)
        if response.status_code >= 400:
            logging.error("HTTP {} from {}".format(response.status_code, response.url))
            return 'ERROR, HTTP {}'.format(response.status_code)
        logging.info("HTTP {} from {}".format(response.status_code, response.url))
        found = Counter(re.findall(SEARCH_REGEX, response.text.lower(), flags=re.IGNORECASE))
        return str(found.most_common(3))
    except requests.exceptions.RequestException as e:
        logging.error("{} from {}".format(e.__class__.__name__, url))
        return e.__class__.__name__


def search_sites(url_queue, output_queue):
    """
    Processes the urls in the given url queue and puts the output into
    the output_queue for asynchronous output processing
    """
    while not url_queue.empty():
        next_site = url_queue.get()
        output_queue.put((next_site, search_site(next_site)))
        url_queue.task_done()


def write_output(output_queue):
    """
    writes formatted output to the gvien OUTPUT_FILE_NAME from the
    output_queue for thread-safe output
    """
    with open(OUTPUT_FILE_NAME, 'w') as output_file:
        while True:
            site, found = output_queue.get()
            output_file.write('{}: {}\n'.format(site, found))
            output_queue.task_done()


def main():
    urls = website_list()  # get the lsit of urls to search
    url_queue = Queue(maxsize=0)  # queue of urls for the workers to process
    map(url_queue.put, urls)  # put the urls into the url_queue
    output_queue = Queue(maxsize=0)  # queue of output from the workers to write to the file

    # start the output thread, it will start outputting to file as soon as output is available
    output_thread = Thread(target=write_output, args=(output_queue,))  # for thread-safe file i/o
    output_thread.daemon = True
    output_thread.start()

    # spawn worker threads
    for _ in range(NTHREADS):
        worker_thread = Thread(target=search_sites, args=(url_queue, output_queue))
        worker_thread.daemon = True
        worker_thread.start()

    url_queue.join()  # wait for the url queue to be fully processed
    output_queue.join()  # then wait for the output queue to be fully processed


if __name__ == '__main__':
    main()
