from urllib.request import Request, urlopen
import urllib.error
import gzip
from bs4 import BeautifulSoup


def get_urls(base_url):
    req = Request(base_url, headers={'User-Agent': 'Mozilla/5.0'})

    try:
        xml = urlopen(req).read()
    except urllib.error.HTTPError:
        return
    soup = BeautifulSoup(xml, features="html.parser")

    # Initialize lists for sitemaps and urls for current sitemap
    sitemap_urls = []
    urls = []

    # Find all sitemaps and urls
    sitemap_tags = soup.find_all("sitemap")
    url_tags = soup.find_all("url")

    # Append links to lists
    for sitemap_tag in sitemap_tags:
        sitemap_urls.append(sitemap_tag.findNext("loc").text)
    for url_tag in url_tags:
        urls.append(url_tag.findNext("loc").text)

    return sitemap_urls, urls


def get_soup(req):
    # Get HTML of compressed page
    try:
        html = gzip.decompress(urlopen(req).read()).decode('utf-8')
    # If isn't compressed
    except gzip.BadGzipFile:
        try:
            html = urlopen(req).read().decode('utf-8')
        except Exception as e:
            return str(e)
    except Exception as e:
        return str(e)

    # Make BeautifulSoup from html
    return BeautifulSoup(html, features="html.parser")
