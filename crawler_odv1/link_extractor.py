import re
from urllib.parse import urljoin, urlparse,urlunparse

def extract_links(base_url, html):
    
    links = set()
    for match in re.findall(r'href=["\'](.*?)["\']', html, flags=re.IGNORECASE):
        
        link = match.strip() 
        link = re.sub(r'^[\s:;]+', '', link)
        
        if link.startswith("https:/") and not link.startswith("https://"):
            link = link.replace("https:/", "https://", 1)
        elif link.startswith("http:/") and not link.startswith("http://"):
            link = link.replace("http:/", "http://", 1)

        link = urljoin(base_url, link)

        if link.startswith("http") and urlparse(link).netloc == urlparse(base_url).netloc:

            if "subtitledreams.com" in base_url:
                page_match = re.search(r'/page/([0-9]+)/', link, re.IGNORECASE)
                if page_match:
                    page_num = int(page_match.group(1))
                    if page_num > 10:
                        continue

            if "dramabeans.com" in base_url:
                page_match = re.search(r'/page/([0-9]+)/', link, re.IGNORECASE)
                if page_match:
                    page_num = int(page_match.group(1))
                    if page_num > 46:
                        continue

            
            if re.search(r'/(members|login|logout|register|sign-up|account|privacy|cart|contact|feed|rss|premium-supporter|dramabeans-privacy-policy|contact-us|search|feeds|faq|comments|comment|about)(/|$)', link, re.IGNORECASE):
                continue

            if re.search(r'\.(css|js|png|jpg|jpeg|gif|ico|svg|pdf|zip|xml|rss|php|mp4|webp|avif)(\?|$)', link, re.IGNORECASE):
                continue
        
            if re.search(r'/tag/', link, re.IGNORECASE):
                continue
            if re.search(r'/wp-', link, re.IGNORECASE):
                continue
            if re.search(r'comment', link, re.IGNORECASE):
                continue

            parsed = urlparse(link)
            parsed = parsed._replace(query="", fragment="")
            link = urlunparse(parsed)

            if link.endswith('/') and not re.match(r'^https?://[^/]+/$', link):
                link = link.rstrip('/')
            links.add(link)
    return links
