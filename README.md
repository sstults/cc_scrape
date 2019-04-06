# cc_scrape

Pulls down gzipped segments from Common Crawl corresponding to a domain

## Running

```
pip install -r requirements.txt
python get_segments.py www.opensourceconnections.com
```

A folder named `cc_temp` will be created and within it will be a bunch of gzip files. Each of those is a WARC file corresponding to the specified domain.

