import requests
import time
import random


USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
    ]

def fetch_html(url_name, delay, timeout=20):
  base = random.choice(USER_AGENTS)
  
  user_agent = f"{base} BasicCrawler/1.0 (study project; contact: xpylypenko@stuba.sk)"

  headers = {
        "User-Agent": user_agent,
        "Accept": "text/html"
    }
  
  try:
    response = requests.get(headers=headers, url=url_name,timeout=timeout)
    time.sleep(delay)

    if response.status_code==200 and "text/html" in response.headers.get("Content-Type",""):
      print(f"ok {url_name}")
      return response.text
    else:
      print(f"skip {url_name} status- {response.status_code}")
      return None
  except requests.exceptions.RequestException as e:
    print(f"Error {url_name} {e}")
    return None


  