import os
import re
import html
import json
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

SRC = "data-filtered/"
DST = "data-extracted/"
os.makedirs(DST, exist_ok=True)
CUSTOM_STOPWORDS = {
    "episode", "episodes", "recap", "review", "reviews", "drama", "kdrama",
    "series", "season", "show", "actor", "actress", "cast", "scene", 
    "story", "plot", "recaps", "tv", "korean"
}

STOPWORDS = ENGLISH_STOP_WORDS.union(CUSTOM_STOPWORDS)

def define_domain(html_text):

    if "kdramafighting" in html_text:
        domain = "kdramafighting.com"
    elif "subtitledreams" in html_text:
        domain = "subtitledreams.com"
    elif "dramabeans" in html_text:
        domain = "dramabeans.com"
    elif "kdramalove" in html_text:
        domain = "kdramalove.com"
    else:
        domain = ""
    return domain

def save_entities_to_json(entities, json_path="entities.jsonl"):
    with open(json_path, "a", encoding="utf-8") as f:
        json.dump(entities, f, ensure_ascii=False)
        f.write("\n")

def detect_page_type(url):
    if "/shows/" in url:
        return "show"

    return "other"


def extract_attributes(html_text, file):
    title_match = re.search(r'<title>(.*?)</title>', html_text, re.I | re.S)
    title = title_match.group(1).strip() if title_match else ""
    title = re.sub(r'\s+', ' ', title).strip()
    # AUTHOR EXTRACTION 
    author = ""

    author_match = re.search(r'<meta[^>]*name=["\']author["\'][^>]*content=["\'](.*?)["\']', html_text, re.I)
    if author_match:
        author = author_match.group(1).strip()

    if not author :
        author_match = re.search(r'<span[^>]*class=["\']author[^>]*>\s*<a[^>]*>(.*?)<', html_text, re.I)
        if author_match:
            author = author_match.group(1).strip()

    if not author:
        author_json_match = re.search(r'"author"\s*:\s*\{[^}]*"name"\s*:\s*"(.*?)"', html_text, re.I)
        if author_json_match:
            author = author_json_match.group(1).strip()

    author = re.sub(r'\s+', ' ', author)

    #  DATE EXTRACTION
    date = ""

    date_match = re.search(
        r'<meta[^>]*property=["\']article:published_time["\'][^>]*content=["\'](.*?)["\']',
        html_text, re.I)
    if date_match:
        date = date_match.group(1).strip()

    if not date:
        date_match = re.search(
        r'<time[^>]*class=["\'](?:entry-date\s+)?published["\'][^>]*datetime=["\'](.*?)["\']',
        html_text, re.I)
        if date_match:
            date = date_match.group(1).strip()
  
    if not date:
        date_json_match = re.search(
            r'"datePublished"\s*:\s*"(.*?)"', html_text, re.I)
        if date_json_match:
            date = date_json_match.group(1).strip()


    date = re.sub(r'["\']', '', date).strip()

    # DESCRIPTION EXTRACTION
    description = ""

    desc_match = re.search(
        r'<meta[^>]*name="description"[^>]*content="([^"]+)"[^>]*>',
        html_text, re.I | re.S
    )
    if desc_match:
        description = desc_match.group(1).strip()

    desc_match = re.search(
        r'<meta[^>]*content="([^"]+)"[^>]*name="description"[^>]*>',
        html_text, re.I | re.S
    )
    if not description and desc_match:
        description = desc_match.group(1).strip()

    if not desc_match:
        desc_match = re.search(
            r'<meta[^>]*property=["\']description["\'][^>]*content=["\'](.*?)["\']',
            html_text, re.I
        )

    if not desc_match:
        desc_match = re.search(
            r'<meta[^>]*content=["\'](.*?)["\'][^>]*property=["\']description["\']',
            html_text, re.I
        )

    if not desc_match:
        desc_match = re.search(
            r'<meta[^>]*property=["\']og:description["\'][^>]*content=["\'](.*?)["\']',
            html_text, re.I
    )
    if not description and desc_match:
        description = desc_match.group(1)
        if description:
            description = description.strip()  
    if not desc_match:
        desc_match = re.search(
            r'<meta[^>]*content=(["\'])([^"\'>]+)\1[^>]*property=(["\'])og:description\3[^>]*>',
            html_text, re.I | re.S
    )
        
    if not description and desc_match:
        description = desc_match.group(2)
        if description:
            description = description.strip()
    


    # URL EXTRACTION
    url = ""

    url_match = re.search(
        r'<meta[^>]*property=["\']og:url["\'][^>]*content=["\'](.*?)["\']',
        html_text, re.I)
    
    if url_match:
        url = url_match.group(1).strip()

    if not url:
        url_match = re.search(
            r'<meta[^>]*content=(["\'])([^"\'>]+)\1[^>]*property=(["\'])og:url\3[^>]*>',
            html_text, re.I | re.S
    )
    if not url and url_match:
        url = url_match.group(2).strip()

    url = url.strip()

    #  TAGS EXTRACTION
    page_type = detect_page_type(url)

    tags = []
    if page_type == "show":
        match = re.search(
            r'<section[^>]*class=["\']banner-recap-detail[^"\']*["\'][^>]*>(.*?)</section>',
            html_text, re.I | re.S
        )
        if match:
            block = match.group(1)
            tags += re.findall(r'<a[^>]*class=["\']post_tags["\'][^>]*>(.*?)</a>', block, re.I)
            tags += re.findall(r'<a[^>]*rel=["\']tag["\'][^>]*>(.*?)</a>', block, re.I)

    else:
        tags += re.findall(r'<a[^>]*class=["\']post_tags["\'][^>]*>(.*?)</a>', html_text, re.I)
        tags += re.findall(r'<a[^>]*rel=["\']tag["\'][^>]*>(.*?)</a>', html_text, re.I)

    tags = [re.sub(r'<[^>]+>', '', t).strip() for t in tags if len(t.strip()) > 1]
    tags = list(dict.fromkeys(tags))
    # tags += re.findall(r'<a[^>]*class=["\']post_tags["\'][^>]*>(.*?)</a>', html_text, re.I)
    # tags += re.findall(r'<a[^>]*rel=["\']tag["\'][^>]*>(.*?)</a>', html_text, re.I)
    # tags = [re.sub(r'<[^>]+>', '', t).strip() for t in tags if len(t.strip()) > 1]
    # tags = list(dict.fromkeys(tags))  

    domain =define_domain(html_text)
    
    entities= {"file":file,
        "domain": domain,
        "title": title,
        "author": author,
        "date": date,
        "description": description,
        "url": url,
        "tags": tags
        }
    save_entities_to_json( entities)
    return entities




def extract_from_html(html_text, attrib):
    description = attrib.get("description", "")
    tags = " ".join(attrib.get("tags", []))

    #  TITLE EXTRACTION
    title_match = re.search(r'<title>(.*?)</title>', html_text, re.I | re.S)
    title = title_match.group(1).strip() if title_match else ""
    title = re.sub(r'[^a-zA-Z0-9가-힣]', ' ', title).lower()
    title = re.sub(r'\s+', ' ', title).strip()
    source = "unknown"

    #  MAIN TEXT EXTRACTION
    main_match = re.search(r'<article[^>]*>(.*?)</article>', html_text, re.I | re.S)
    source = "article"
    if not main_match:
        main_match = re.search(r'<div id="content"[^>]*>(.*?)<div class="comment-content"[^>]*>', html_text, re.S | re.I)
        source = "div"
        if not main_match:
            main_match = re.search(r'<main[^>]*>(.*?)</main>', html_text, re.I | re.S)
            source = "main"
            if not main_match:
                main_match = re.search(r'<body[^>]*>(.*?)</body>', html_text, re.I | re.S)
                source = "body"
            
    main_text = main_match.group(0) if main_match else ""

    main_text = re.sub(r'(?is)<(script|style).*?>.*?</\1>', '', main_text)
    main_text = re.sub(r'<!--.*?-->', '', main_text)

    main_text = re.sub(r'(?is)<(aside|nav|form|figure|time).*?>.*?</\1>', '', main_text)

    main_text = re.sub(r'(?is)<img[^>]*>', '', main_text)
    main_text = re.sub(r'<[^>]+>', ' ', main_text)
    main_text = re.sub(r'[^a-zA-Z0-9가-힣]', ' ', main_text)
    main_text = re.sub(r'\s+', ' ', main_text).strip().lower()

    description = re.sub(r'[^a-zA-Z0-9가-힣]', ' ', description)
    description = re.sub(r'\s+', ' ', description).strip().lower()

    tags = re.sub(r'[^a-zA-Z0-9가-힣]', ' ', tags)
    tags = re.sub(r'\s+', ' ', tags).strip().lower()

    tokens= [ t for t in main_text.split(' ') if t not in STOPWORDS 
        and (
        not t.isdigit()
        or (t.isdigit() and 1 <= int(t) <= 50)
        or t in [f"0{i}" for i in range(1, 10)]
        or (t.isdigit() and 1880 <= int(t) <= 2030))
        and not (t.isdigit() and len(t) > 2 and not (1880 <= int(t) <= 2030))]
    
    main_text = ' '.join(tokens)
    full_text = f"{title} {description} {main_text} {tags}".strip()

    print(f"[{source:10}] extracted")

    return full_text


def main():
    total, extracted = 0, 0

    for root, _, files in os.walk(SRC):
        for file in files:
            if not file.endswith(".html"):
                continue

            total += 1
            filepath = os.path.join(root, file)
            print(f"{filepath} - ")
            with open(filepath, encoding="utf-8", errors="ignore") as f:
                html_text = f.read()
            html_text =html.unescape(html_text)
            #  ATTRIBUTES EXTRACTION
            attrib = extract_attributes(html_text, file)
            #  CLEAN TEXT EXTRACTION
            clean_text = extract_from_html(html_text, attrib)

            extracted += 1

            relative_path = os.path.relpath(root, SRC)
            dest_dir = os.path.join(DST, relative_path)
            os.makedirs(dest_dir, exist_ok=True)

            outpath = os.path.join(dest_dir, file.replace(".html", ".txt"))
            with open(outpath, "w", encoding="utf-8") as out:
                out.write(clean_text)

    print(f"Extracted {extracted}/{total}")

if __name__ == "__main__":
    main()
