# crawler.py
from fetcher import fetch_html
from save import save_page
from link_extractor import extract_links
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse
from time import sleep
import re
import random

seed_urls = [
    "https://dramabeans.com/",
    "https://www.kdramafighting.com/",
    "https://subtitledreams.com/",

]
robots_cache = {}  

def is_allowed_by_robots(url, user_agent="*"):

    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    if base not in robots_cache:
        rp = RobotFileParser()
        robots_url = f"{base}/robots.txt"
        rp.set_url(robots_url)
        try:
            rp.read()
            print(f"Fetched {robots_url} OK")
        except Exception as e:
            print(f"WARN Could not read robots.txt for {base}: {e}")
        robots_cache[base] = rp

    rp = robots_cache[base]
    allowed = rp.can_fetch(user_agent, url)
    return allowed


def crawl(seed_urls, max_pages, max_depth):
    visited=set()
    unvisited=[(url,0) for url in seed_urls]

    while unvisited and len(visited) < max_pages:
        url, depth = unvisited.pop(0)
        if url in visited or depth > max_depth:
            continue
        if not "dramabeans.com" in url:
            if not is_allowed_by_robots(url, "*"):
             print(f"BLOCKED by robots.txt {url}")
             continue

        delay = random.uniform(3, 10)
        html = fetch_html(url,delay)

        if html:
            print(f"[{len(visited)+1}/{max_pages}] Depth={depth} | Fetching: {url}")
            save_page(url,html, 200, depth)
            visited.add(url)

            if depth < max_depth:
                new_links=extract_links(url,html)
                for link in new_links:
                    if link not in visited and all(link != u for u, _ in unvisited):
                        if re.search(r'/page/[0-9]+/', link, re.IGNORECASE):
                            unvisited.append((link, depth))
                        else:
                            unvisited.append((link, depth + 1))
        else:
            print(f"FAIL could not fetch: {url}")
        sleep(1)
    print(f"\nCrawling finished. Total pages fetched: {len(visited)}")
if __name__ == "__main__":
    crawl(["https://subtitledreams.com/"], max_pages=5000, max_depth=20)
    crawl(["https://www.kdramafighting.com/"], max_pages=3000, max_depth=10)
    crawl(["https://dramabeans.com/"], max_pages=10000, max_depth=35)
    crawl(["https://www.kdramalove.com/reviews.html"], max_pages=1000, max_depth=6)

