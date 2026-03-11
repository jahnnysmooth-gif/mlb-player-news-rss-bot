import os
import re
import time
import html
import hashlib
from datetime import datetime, UTC, timedelta
from urllib.parse import urlparse, urlunparse, urljoin
from zoneinfo import ZoneInfo

import feedparser
import requests
import redis
from bs4 import BeautifulSoup

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
REDIS_URL = os.getenv("REDIS_URL", "")
MAX_POSTS_PER_RUN = 8
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
MAX_NEWS_AGE_HOURS = int(os.getenv("MAX_NEWS_AGE_HOURS", "24"))
DEDUP_TTL_DAYS = int(os.getenv("DEDUP_TTL_DAYS", "14"))

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)

TEAM_ABBR = [
    "ARI","ATL","BAL","BOS","CHC","CWS","CIN","CLE","COL","DET",
    "HOU","KC","LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK",
    "PHI","PIT","SD","SF","SEA","STL","TB","TEX","TOR","WSH"
]

FEEDS = [
    {
        "name": "RotoWire",
        "key": "rotowire",
        "type": "rss",
        "url": "https://www.rotowire.com/rss/news.php?sport=MLB",
        "priority": 1,
    },
    {
        "name": "FantasyPros",
        "key": "fantasypros",
        "type": "html",
        "url": "https://www.fantasypros.com/mlb/player-news.php",
        "priority": 2,
    },
    {
        "name": "MLB Trade Rumors Transactions",
        "key": "mlbtr_transactions",
        "type": "rss",
        "url": "https://www.mlbtraderumors.com/transactions/feed",
        "priority": 3,
    },
]


def extract_team(text):
    t = text.upper()
    for abbr in TEAM_ABBR:
        if f" {abbr} " in f" {t} ":
            return abbr
    return None


def normalize(text):
    return re.sub(r"\s+", " ", text or "").strip()


def strip_html(text):
    text = html.unescape(text or "")
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return normalize(text)


def extract_player(text):
    text = normalize(text)

    patterns = [
        r"\b([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+(?:Jr\.|Sr\.|II|III))?)\b",
    ]

    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            return normalize(m.group(1))

    return None


def classify_news(text):
    t = text.lower()

    if "injur" in t or "il" in t:
        return "🚑", "Injury"

    if "lineup" in t or "scratched" in t:
        return "🔄", "Lineup"

    if "closer" in t or "save chance" in t:
        return "🔒", "Bullpen"

    if "called up" in t or "promoted" in t:
        return "⬆️", "Call-Up"

    if "traded" in t or "signed" in t or "dfa" in t:
        return "🚨", "Transaction"

    return "📰", "Player News"


def color_for_tag(tag):
    return {
        "Injury": 0xE74C3C,
        "Lineup": 0x3498DB,
        "Bullpen": 0x9B59B6,
        "Call-Up": 0x2ECC71,
        "Transaction": 0xF39C12,
        "Player News": 0x95A5A6,
    }.get(tag, 0x95A5A6)


def parse_rss_date(entry):
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return datetime(*entry.published_parsed[:6], tzinfo=UTC)

    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        return datetime(*entry.updated_parsed[:6], tzinfo=UTC)

    return None


def is_recent(dt):
    if not dt:
        return False

    return datetime.now(UTC) - dt < timedelta(hours=MAX_NEWS_AGE_HOURS)


def canonical_link(link):
    link = normalize(link)

    if not link:
        return ""

    parsed = urlparse(link)
    cleaned = parsed._replace(query="", fragment="")

    return urlunparse(cleaned)


def dedupe_key(item):
    raw = f"{item['source_key']}||{item['title']}||{item['summary']}"
    digest = hashlib.sha256(raw.encode()).hexdigest()

    return f"mlb-news:{digest}"


def fetch_rss_feed(source):
    parsed = feedparser.parse(source["url"])

    items = []

    for entry in parsed.entries[:25]:

        published = parse_rss_date(entry)

        if not is_recent(published):
            continue

        title = normalize(getattr(entry, "title", ""))
        summary = strip_html(getattr(entry, "summary", ""))
        link = canonical_link(getattr(entry, "link", ""))

        items.append({
            "title": title,
            "summary": summary,
            "link": link,
            "source_name": source["name"],
            "source_key": source["key"],
            "priority": source["priority"],
            "published": published,
        })

    return items


def fetch_fantasypros_feed(source):

    response = requests.get(
        source["url"],
        timeout=HTTP_TIMEOUT,
        headers={"User-Agent": USER_AGENT},
    )

    soup = BeautifulSoup(response.text, "html.parser")

    page_text = soup.get_text("\n")

    items = []

    for block in page_text.split("\n\n"):

        title = normalize(block.split("\n")[0])

        if not title:
            continue

        summary = normalize(block)

        items.append({
            "title": title,
            "summary": summary,
            "link": source["url"],
            "source_name": source["name"],
            "source_key": source["key"],
            "priority": source["priority"],
            "published": datetime.now(UTC),
        })

    return items


def fetch_feed(source):

    if source["type"] == "rss":
        return fetch_rss_feed(source)

    if source["type"] == "html":
        return fetch_fantasypros_feed(source)

    return []


def post_to_discord(item):

    emoji, tag = classify_news(item["title"] + " " + item["summary"])

    player = extract_player(item["title"]) or extract_player(item["summary"])
    team = extract_team(item["title"] + " " + item["summary"])

    if player and team:
        header = f"{emoji} {player} ({team})"
    elif player:
        header = f"{emoji} {player}"
    else:
        header = f"{emoji} Player News"

    details = item["summary"][:420]

    payload = {
        "embeds": [
            {
                "title": header,
                "url": item["link"],
                "description": details,
                "color": color_for_tag(tag),
                "fields": [
                    {"name": "Tag", "value": tag, "inline": True},
                    {"name": "Source", "value": item["source_name"], "inline": True},
                ],
                "footer": {"text": "MLB Player News Bot"},
                "timestamp": datetime.now(UTC).isoformat(),
            }
        ]
    }

    r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=HTTP_TIMEOUT)

    if r.status_code >= 300:
        r.raise_for_status()

    time.sleep(1.5)


def get_redis():
    return redis.from_url(REDIS_URL, decode_responses=True)


def main():

    rdb = get_redis()

    raw_items = []

    for source in FEEDS:

        try:
            items = fetch_feed(source)

            raw_items.extend(items)

            print(f"{source['name']}: fetched {len(items)} items")

        except Exception as exc:
            print(f"{source['name']} failed: {exc}")

    posted = 0

    for item in raw_items:

        key = dedupe_key(item)

        if rdb.exists(key):
            continue

        try:

            post_to_discord(item)

            ttl = DEDUP_TTL_DAYS * 24 * 60 * 60

            rdb.setex(key, ttl, "1")

            posted += 1

        except Exception as e:

            print("Failed posting", item["title"], e)

        if posted >= MAX_POSTS_PER_RUN:
            break

    print("Posted", posted)


if __name__ == "__main__":
    main()
