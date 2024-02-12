# Import
import awswrangler as wr
from bs4 import BeautifulSoup
import pandas as pd
import re
import urllib3

# Parameters
wiki_url = "https://en.wikipedia.org/wiki/List_of_busiest_container_ports"
chunk_size = 24
s3_raw_output_path = "s3://dev-raw-container-port/ports.csv"

# Functions
def parse_wiki(wiki_url):
    http = urllib3.PoolManager()
    wiki_page_request = http.request("GET", wiki_url)
    soup = BeautifulSoup(wiki_page_request.data, 'html.parser')
    return soup

def append_headers(soup):
    headers = []
    tclass = soup('table', {"class":"wikitable sortable"})[0:]

    for temp in tclass:
        for t_temp in temp.find_all('th'):
            column_name = re.sub('\[\d{1,3}\]','',t_temp.text.strip())
            headers.append(column_name)
    return headers
  
def append_rows(soup):
    rows = []
    tclass = soup('table', {"class":"wikitable sortable"})[0:]
    for temp in tclass:
        for t_temp in temp.find_all('td'):
            row = re.sub('\[\d{1,3}\]','',t_temp.text.strip())
            rows.append(row)
    return rows

def append_chunked_list(rows):
    chunked_list = list()
    for i in range (0, len(rows),chunk_size):
        chunked_list.append(rows[i:i+chunk_size])
    return chunked_list

# Run functions
soup = parse_wiki(wiki_url=wiki_url)
headers = append_headers(soup=soup)
rows = append_rows(soup=soup)
chunked_list = append_chunked_list(rows=rows)

# Create df
df = pd.DataFrame(chunked_list,columns=headers)
df = df[:10]

# Export csv to S3 raw
wr.s3.to_csv(
    df = df,
    path= s3_raw_output_path)