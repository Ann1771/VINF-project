import os
import shutil
from collections import defaultdict
import re

SRC = "data/"
DST = "data-filtered/"
tr ="unfiltered/"
os.makedirs(DST, exist_ok=True)

#  for filtering pages based on url
BLOCK_LIST = {
    "dramabeans": [
        "weekly-squee", "were-watching", "treasure-hunt", "_open-thread", "beanie",
        "vote-for-your-favorite", "movie-review","k-movie-night","name-that-drama","celebs","news","caption-this","drama-chat","_resources","drama-hangout","page","thoughts","rate-and-review","you-can-pick-one","advertise","blog-view","_recaps","_extras","_ratings","recaps_all","_view-subscription"
    ],

    "kdramafighting": [
        "bingo", "lookalike", "trends", "music","article","faq","lists",
        "vivi", "coco", "meet-", "fashion-face-off"
    ],

    "subtitledreams": [
        "chinese-drama", "author", "bl-", "a-z-list", "movie", "page","book","music","category","k-musings"
    ],

    "kdramalove": [
        "review", "gallery","p_reviews_html"
    ],
}

def detect_domain(filepath):
    for domain in BLOCK_LIST.keys():
        if domain in filepath.lower():
            return domain
    return "unknown"

def should_skip(filepath, domain):
    if os.path.getsize(filepath) < 500:
        return True

    name = os.path.basename(filepath).lower()

    if re.match(r'^(www_)?(dramabeans|kdramafighting|kdramalove|subtitledreams)_com(_\d{0,4})?(_\d{0,2})?_?(\.html)?$', name):
      return True

    for phrase in BLOCK_LIST.get(domain, []):
        if phrase in name:
            return True
        

    #  filtering base on html 
    if "subtitledreams" in filepath.lower():
        with open(filepath, encoding="utf-8", errors="ignore") as f:
            html_text = f.read()

        # Skip if not marked as Korean drama
        if not re.search(r'(category-korean-drama|category-k-drama-reviews|tag-gay-korean-drama|tag-k-drama|tag-korean-drama|tag-k-drama-review|tag-korean-drama-review)', html_text, re.I):
            
            return True
    return False

def main():
    stats = defaultdict(lambda: {"kept": 0, "total": 0})

    for root, _, files in os.walk(SRC):
        for file in files:
            if not file.endswith(".html"):
                continue

            filepath = os.path.join(root, file)
            domain = detect_domain(filepath)
            stats[domain]["total"] += 1

            if should_skip(filepath, domain):
                continue

           
            relative_path = os.path.relpath(root, SRC)
            dest_dir = os.path.join(DST, relative_path)
            os.makedirs(dest_dir, exist_ok=True)

            shutil.copy(filepath, os.path.join(dest_dir, file))
            stats[domain]["kept"] += 1

    print("\nFiltering summary:")
    print("-" * 40)
    for d, s in stats.items():
        print(f"{d:15} kept {s['kept']:4}/{s['total']:4} pages")
    print("-" * 40)
    print("Done!")

if __name__ == "__main__":
    main()