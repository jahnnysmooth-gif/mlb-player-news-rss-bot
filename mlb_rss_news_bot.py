import os
import re
import time
import html
import hashlib
import sqlite3
from datetime import datetime, UTC, timedelta
from urllib.parse import urlparse, urlunparse

import feedparser
import requests

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
DB_FILE = os.getenv("DB_FILE", "mlb_rss_news.db")
MAX_POSTS_PER_RUN = int(os.getenv("MAX_POSTS_PER_RUN", "3"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))

# only post news newer than this
MAX_NEWS_AGE_HOURS = 3

FEEDS = [
    {
        "name": "RotoWire",
        "key": "rotowire",
        "url": "https://www.rotowire.com/rss/news.php?sport=MLB",
        "priority": 1,
    },
    {
        "name": "MLB Trade Rumors Transactions",
        "key": "mlbtr_transactions",
        "url": "https://www.mlbtraderumors.com/transactions/feed",
        "priority": 2,
    },
]

PLAYER_NEWS_KEYWORDS = [
    "placed on",
    "il",
    "day-to-day",
    "scratched",
    "starting",
    "returns to the lineup",
    "batting",
    "rehab assignment",
    "mri",
    "forearm",
    "elbow",
    "shoulder",
    "hamstring",
    "oblique",
    "activated",
    "reinstated",
    "optioned",
    "recalled",
    "called up",
    "promoted",
    "dfa",
    "released",
    "traded",
    "signed",
    "acquired",
    "closer",
    "save chance",
]

ARTICLE_PATTERNS = [
    r"\bpreview\b",
    r"\branking\b",
    r"\bdepth chart\b",
    r"\broundup\b",
    r"\brecap\b",
    r"\bmailbag\b",
]

INJURY_WORDS = ["injured", "il", "mri", "tightness", "soreness"]
LINEUP_WORDS = ["lineup", "starting", "scratched"]
CLOSER_WORDS = ["closer", "save chance", "bullpen"]
CALLUP_WORDS = ["called up", "promoted", "recalled"]
TRANSACTION_WORDS = ["traded", "signed", "dfa", "released", "activated"]


def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS posted_items (
            dedupe_key TEXT PRIMARY KEY,
            posted_at TEXT
        )
    """)
    conn.commit()
    return conn


def was_posted(conn, key):
    r = conn.execute("SELECT 1 FROM posted_items WHERE dedupe_key = ?", (key,))
    return r.fetchone() is not None


def mark_posted(conn, key):
    conn.execute(
        "INSERT OR REPLACE INTO posted_items VALUES (?, ?)",
        (key, datetime.now(UTC).isoformat()),
    )
    conn.commit()


def normalize(text):
    return re.sub(r"\s+", " ", text or "").strip()


def strip_html(text):
    text = html.unescape(text or "")
    text = re.sub(r"<[^>]+>", " ", text)
    return normalize(text)


def canonical_link(link):
    parsed = urlparse(link)
    cleaned = parsed._replace(query="", fragment="")
    return urlunparse(cleaned)


def extract_player(text):
    pattern = r"\b([A-Z][a-z]+\s[A-Z][a-z]+(?:\s(?:Jr\.|II|III))?)\b"
    m = re.search(pattern, text)
    if m:
        return normalize(m.group(1))
    return None


def contains_keyword(text):
    t = text.lower()
    return any(k in t for k in PLAYER_NEWS_KEYWORDS)


def classify_news(text):
    t = text.lower()

    if any(w in t for w in INJURY_WORDS):
        return "🚑", "Injury"

    if any(w in t for w in LINEUP_WORDS):
        return "🔄", "Lineup"

    if any(w in t for w in CLOSER_WORDS):
        return "🔒", "Bullpen"

    if any(w in t for w in CALLUP_WORDS):
        return "⬆️", "Call-Up"

    if any(w in t for w in TRANSACTION_WORDS):
        return "🚨", "Transaction"

    return "📰", "Player News"


def parse_date(entry):
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return datetime(*entry.published_parsed[:6], tzinfo=UTC)
    return None


def is_recent(dt):
    if not dt:
        return False
    return datetime.now(UTC) - dt < timedelta(hours=MAX_NEWS_AGE_HOURS)


def fetch_feed(source):
    parsed = feedparser.parse(source["url"])
    items = []

    for entry in parsed.entries[:25]:

        published = parse_date(entry)

        if not is_recent(published):
            continue

        title = normalize(entry.title)
        summary = strip_html(getattr(entry, "summary", ""))
        link = canonical_link(entry.link)

        items.append({
            "title": title,
            "summary": summary,
            "link": link,
            "source": source["name"],
            "priority": source["priority"],
            "published": published,
        })

    return items


def dedupe_key(player, title):
    raw = f"{player.lower()}-{title.lower()}"
    return hashlib.sha256(raw.encode()).hexdigest()


def post_to_discord(item):

    emoji, tag = classify_news(item["title"] + " " + item["summary"])

    payload = {
        "username": "MLB Player News",
        "embeds": [
            {
                "title": f"{emoji} {item['player']}",
                "url": item["link"],
                "description": f"**{item['title']}**\n\n{item['summary'][:800]}",
                "footer": {"text": item["source"]},
                "timestamp": datetime.now(UTC).isoformat(),
            }
        ],
    }

    for _ in range(5):

        r = requests.post(DISCORD_WEBHOOK_URL, json=payload)

        if r.status_code < 300:
            time.sleep(1.5)
            return

        if r.status_code == 429:
            retry = r.json().get("retry_after", 5)
            print(f"Rate limited. Sleeping {retry}")
            time.sleep(retry)
            continue

        r.raise_for_status()


def main():

    conn = init_db()

    raw_items = []

    for source in FEEDS:
        items = fetch_feed(source)
        raw_items.extend(items)
        print(f"{source['name']}: fetched {len(items)} items")

    valid = []

    for item in raw_items:

        player = extract_player(item["title"]) or extract_player(item["summary"])

        if not player:
            continue

        if not contains_keyword(item["title"] + item["summary"]):
            continue

        item["player"] = player
        valid.append(item)

    valid.sort(key=lambda x: x["priority"])

    print(f"Eligible player-news items found: {len(valid)}")

    posted = 0

    for item in valid:

        key = dedupe_key(item["player"], item["title"])

        if was_posted(conn, key):
            continue

        try:
            post_to_discord(item)
            mark_posted(conn, key)
            posted += 1
        except Exception as e:
            print("Failed posting", item["title"], e)

        if posted >= MAX_POSTS_PER_RUN:
            break

    print("Posted", posted)


if __name__ == "__main__":
    main()
