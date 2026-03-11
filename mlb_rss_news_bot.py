import os
import re
import time
import html
import hashlib
from datetime import datetime, UTC, timedelta
from urllib.parse import urlparse, urlunparse

import feedparser
import requests
import redis

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
REDIS_URL = os.getenv("REDIS_URL", "")
MAX_POSTS_PER_RUN = int(os.getenv("MAX_POSTS_PER_RUN", "3"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
MAX_NEWS_AGE_HOURS = int(os.getenv("MAX_NEWS_AGE_HOURS", "24"))
DEDUP_TTL_DAYS = int(os.getenv("DEDUP_TTL_DAYS", "14"))

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
    r"\bmailbag\b",
    r"\bpodcast\b",
]

INJURY_WORDS = ["injured", "injury", "il", "mri", "tightness", "soreness", "forearm", "elbow", "shoulder", "hamstring", "oblique", "back"]
LINEUP_WORDS = ["lineup", "starting", "scratched", "batting", "leadoff", "cleanup"]
CLOSER_WORDS = ["closer", "save chance", "bullpen"]
CALLUP_WORDS = ["called up", "promoted", "recalled", "optioned"]
TRANSACTION_WORDS = ["traded", "signed", "dfa", "released", "activated", "reinstated", "acquired"]

TEAM_WORDS = {
    "diamondbacks", "braves", "orioles", "red sox", "cubs", "white sox", "reds",
    "guardians", "rockies", "tigers", "astros", "royals", "angels", "dodgers",
    "marlins", "brewers", "twins", "mets", "yankees", "athletics", "a's",
    "phillies", "pirates", "padres", "giants", "mariners", "cardinals", "rays",
    "rangers", "blue jays", "nationals"
}


def get_redis():
    if not REDIS_URL:
        raise RuntimeError("REDIS_URL is not set")
    return redis.from_url(REDIS_URL, decode_responses=True)


def dedupe_exists(rdb, key: str) -> bool:
    return bool(rdb.exists(key))


def dedupe_mark(rdb, key: str) -> None:
    ttl_seconds = DEDUP_TTL_DAYS * 24 * 60 * 60
    rdb.setex(key, ttl_seconds, datetime.now(UTC).isoformat())


def normalize(text):
    return re.sub(r"\s+", " ", text or "").strip()


def strip_html(text):
    text = html.unescape(text or "")
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return normalize(text)


def canonical_link(link):
    link = normalize(link)
    if not link:
        return ""
    parsed = urlparse(link)
    cleaned = parsed._replace(query="", fragment="")
    return urlunparse(cleaned)


def extract_player(text):
    text = normalize(text)

    patterns = [
        r"\b([A-Z][a-z]+(?:[-'][A-Z]?[a-z]+)?\s+[A-Z][a-z]+(?:[-'][A-Z]?[a-z]+)?(?:\s+(?:Jr\.|Sr\.|II|III|IV))?)\b",
        r"\b([A-Z][a-z]+(?:[-'][A-Z]?[a-z]+)?\s+[A-Z]\.\s+[A-Z][a-z]+(?:[-'][A-Z]?[a-z]+)?)\b",
    ]

    for pattern in patterns:
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
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        return datetime(*entry.updated_parsed[:6], tzinfo=UTC)
    return None


def is_recent(dt):
    if not dt:
        return True
    return datetime.now(UTC) - dt < timedelta(hours=MAX_NEWS_AGE_HOURS)


def normalize_for_dedupe(text):
    text = text.lower()
    text = re.sub(r"injured list", "il", text)
    text = re.sub(r"designated for assignment", "dfa", text)
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    return normalize(text)


def summarize_event_text(text):
    t = normalize_for_dedupe(text)

    patterns = [
        r"(placed on .*? il)",
        r"(activated)",
        r"(reinstated)",
        r"(optioned)",
        r"(recalled)",
        r"(called up)",
        r"(promoted)",
        r"(dfa)",
        r"(released)",
        r"(traded)",
        r"(signed)",
        r"(acquired)",
        r"(scratched)",
        r"(starting)",
        r"(returns to the lineup)",
        r"(batting leadoff)",
        r"(batting second)",
        r"(mri)",
        r"(forearm)",
        r"(elbow)",
        r"(shoulder)",
        r"(hamstring)",
        r"(oblique)",
        r"(closer)",
        r"(save chance)",
    ]

    for pattern in patterns:
        m = re.search(pattern, t)
        if m:
            return m.group(1)

    return t


def dedupe_key(player, item):
    if item["source_key"] == "mlbtr_transactions":
        raw_event = summarize_event_text(item["title"] + " " + item["summary"])
    else:
        raw_event = normalize_for_dedupe(item["title"])

    raw = f"{player.lower()}||{raw_event}"
    digest = hashlib.sha256(raw.encode()).hexdigest()
    return f"mlb-news:{digest}"


def looks_like_article(title, summary):
    combined = f"{title} {summary}".lower()
    for pattern in ARTICLE_PATTERNS:
        if re.search(pattern, combined, flags=re.I):
            return True
    return False


def fetch_feed(source):
    parsed = feedparser.parse(source["url"])
    items = []

    for entry in parsed.entries[:25]:
        published = parse_date(entry)
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


def is_valid_item(item):
    title = item["title"]
    summary = item["summary"]
    source_key = item["source_key"]

    if looks_like_article(title, summary):
        return False, None

    player = extract_player(title) or extract_player(summary)
    if not player:
        return False, None

    if source_key == "rotowire":
        if len(summary) < 25:
            return False, None

        summary_lower = summary.lower()
        blocked_rotowire_patterns = [
            "top ",
            "rankings",
            "depth chart",
            "spring training battle",
            "opening day roster",
            "podcast",
            "mailbag",
        ]
        if any(p in summary_lower for p in blocked_rotowire_patterns):
            return False, None

        return True, player

    combined = f"{title} {summary}"
    if not contains_keyword(combined):
        return False, None

    return True, player


def choose_items(raw_items):
    chosen = {}
    seen_links = set()

    for item in raw_items:
        ok, player = is_valid_item(item)
        if not ok:
            continue

        if item["link"] and item["link"] in seen_links:
            continue

        item["player"] = player
        key = dedupe_key(player, item)
        item["dedupe_key"] = key

        if key not in chosen or item["priority"] < chosen[key]["priority"]:
            chosen[key] = item

        if item["link"]:
            seen_links.add(item["link"])

    final_items = list(chosen.values())
    final_items.sort(key=lambda x: (x["priority"], x["title"].lower()))
    return final_items


def post_to_discord(item):
    emoji, tag = classify_news(item["title"] + " " + item["summary"])

    description = f"**{item['title']}**"
    if item["summary"] and item["summary"].lower() != item["title"].lower():
        description += f"\n\n{item['summary'][:1000]}"

    payload = {
        "embeds": [
            {
                "title": f"{emoji} {item['player']}",
                "url": item["link"],
                "description": description,
                "color": 3447003,
                "fields": [
                    {"name": "Tag", "value": tag, "inline": True},
                    {"name": "Source", "value": item["source_name"], "inline": True},
                ],
                "footer": {"text": "RSS player news"},
                "timestamp": datetime.now(UTC).isoformat(),
            }
        ],
    }

    for _ in range(5):
        r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=HTTP_TIMEOUT)

        if r.status_code < 300:
            time.sleep(1.5)
            return

        if r.status_code == 429:
            retry = 5
            try:
                retry = float(r.json().get("retry_after", 5))
            except Exception:
                pass
            print(f"Rate limited. Sleeping {retry}")
            time.sleep(retry + 0.5)
            continue

        r.raise_for_status()


def main():
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL is not set")

    rdb = get_redis()
    raw_items = []

    for source in FEEDS:
        items = fetch_feed(source)
        raw_items.extend(items)
        print(f"{source['name']}: fetched {len(items)} items")

    valid = choose_items(raw_items)

    rotowire_count = sum(1 for x in valid if x["source_key"] == "rotowire")
    mlbtr_count = sum(1 for x in valid if x["source_key"] == "mlbtr_transactions")

    print(f"Eligible player-news items found: {len(valid)}")
    print(f"RotoWire eligible items: {rotowire_count}")
    print(f"MLBTR eligible items: {mlbtr_count}")

    posted = 0

    for item in valid:
        key = item["dedupe_key"]

        if dedupe_exists(rdb, key):
            continue

        try:
            post_to_discord(item)
            dedupe_mark(rdb, key)
            posted += 1
        except Exception as e:
            print("Failed posting", item["title"], e)

        if posted >= MAX_POSTS_PER_RUN:
            break

    print("Posted", posted)


if __name__ == "__main__":
    main()
