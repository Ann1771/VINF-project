import os, csv
import re
from datetime import datetime
from urllib.parse import urlparse
def save_page(url, html, status, depth, base_path="data"):

  domain = urlparse(url).netloc.replace("www.", "")
  path = f"{base_path}/{domain}"

  os.makedirs(f"{path}/html_pages", exist_ok =True)
  os.makedirs(f"{path}", exist_ok=True)

  name = re.sub(r'[^A-Za-z0-9_-]+', '_', url.replace("https://", "").replace("http://", ""))[:80]
  filename=f"{path}/html_pages/{name}.html"

  with open(filename,"w",encoding="utf-8") as f:
    f.write(html or "")

  tsv_path = f"{path}/pages.tsv"

  with open(tsv_path,"a",encoding="utf-8",newline="") as f:
    writer = csv.writer(f,delimiter="\t")

    if f.tell()==0:
      writer.writerow(["url", "status", "file", "length","depth", "datetime"])
    
    writer.writerow([
        url,
        status,
        filename,
        len(html or ""),
        depth,
        datetime.now().isoformat(timespec="seconds")
    ])