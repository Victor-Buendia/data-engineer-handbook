import random
from datetime import datetime, timedelta
import time
import json
from typing import Dict, List
from faker import Faker

fake = Faker()

def generate_events(num_events: int = 10) -> List[Dict]:
    """
    Generate sample web traffic events with fake data.
    """
    hosts = ["www.zachwilson.tech", "admin.zachwilson.tech", "www.eczachly.com"]
    urls = [
        "/",
        "/wp-login.php",
        "/robots.txt",
        "/ads.txt",
        "/app-ads.txt",
        "/.well-known/apple-app-site-association",
        "/.well-known/nodeinfo",
        "/?author=4",
        "/?author=5",
        "/blog",
        "/about",
        "/contact",
        "/signup",
        "/login",
        "/search",
        "/creator"
    ]
    
    referrers = [
        "http://www.eczachly.com/wp-login.php",
        "https://www.google.com/url?q=https://www.zachwilson.tech/&sa=U&sqi=2&ved=2ahUKEwiUuYG5v6r8AhVwpnIEHZanCfwQFnoECAUQAQ&usg=AOvVaw31mc0-g6BhyfdBiIvy4noS",
        "https://www.google.com/search?q=zach+wilson+books+data+engineering",
        "https://www.google.de/",
        "http://admin.zachwilson.tech/robots.txt",
        "http://54.91.59.199:80",
        "http://use.fontawesome.com/",
        "https://www.zachwilson.tech/blog/so-what-exactly-is-a-data-engineer",
        "https://www.eczachly.com/blog/life-of-a-silicon-valley-big-data-engineer",
        "https://www.zachwilson.tech/signup",
        "https://www.inoreader.com/",
        "https://google.com/",
        "https://t.co/",
        "https://www.google.com.hk/",
        "https://www.bing.com",
        "https://www.reddit.com/",
        "https://search.brave.com/",
        "https://www.google.co.in/",
        "https://www.ecosia.org/",
        "http://m.facebook.com/",
        "http://baidu.com/",
        "https://m.youtube.com/",
        "https://www.linkedin.com/",
        "https://yandex.ru/",
        "https://duckduckgo.com/",
        "https://www.youtube.com/",
        "android-app://com.linkedin.android/",
        "",  # Empty referrer for direct visits
    ]
    
    events = []
    
    for _ in range(num_events):
        event = {
            "url": random.choice(urls),
            "referrer": random.choice(referrers),
            "user_agent": fake.user_agent(),
            "host": random.choice(hosts),
            "ip": fake.ipv4(),
            "headers": json.dumps({
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "DNT": "1" if random.random() > 0.5 else "0",
                "Cache-Control": random.choice(["max-age=0", "no-cache"])
            }),
            "event_time": (datetime.now() + timedelta(seconds=random.randint(-300, 300))).strftime("%Y-%m-%d %H:%M:%S.%f")
        }
        events.append(event)
    
    return events

if __name__ == "__main__":
    sample_events = generate_events()
    for event in sample_events:
        print(json.dumps(event))