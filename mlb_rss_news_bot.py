import os
import re
import time
import html
import hashlib
import sqlite3
from datetime import datetime, UTC

import feedparser
import requests

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
DB_FILE = os.getenv("DB_FILE", "mlb_rss_news.db")
MAX_POSTS_PER_RUN = int(os.getenv("MAX_POSTS_PER_RUN", "10"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))

FEEDS = [
    {
        "name": "RotoWire",
        "key": "rotowire",
        "url": "https://www.rotowire.com/rss/news/mlb.php",
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
    "10-day il",
    "15-day il",
    "60-day il",
    "injured list",
    "day-to-day",
    "scratched",
    "starting",
    "not in the lineup",
    "returns to the lineup",
    "batting leadoff",
    "batting second",
    "batting third",
    "cleanup",
    "will start",
    "expected to start",
    "rehab assignment",
    "mri",
    "forearm",
    "elbow",
    "shoulder",
    "hamstring",
    "oblique",
    "back tightness",
    "activated",
    "reinstated",
    "optioned",
    "recalled",
    "called up",
    "promoted",
    "selected the contract",
    "designated for assignment",
    "dfa",
    "released",
    "traded",
    "signed",
    "acquired",
    "closer",
    "save chance",
    "bullpen",
]

ARTICLE_PATTERNS = [
    r"\btop\s+\d+\b",
    r"\bpreview\b",
    r"\branking[s]?\b",
    r"\bdepth chart\b",
    r"\bpower ranking[s]?\b",
    r"\bnotes\b",
    r"\broundup\b",
    r"\brecap\b",
    r"\bwhat we learned\b",
    r"\bhow to watch\b",
    r"\broster battle\b",
    r"\bspring training battle\b",
    r"\bopening day roster\b",
    r"\bmailbag\b",
    r"\bpodcast\b",
    r"\bcolumn\b",
    r"\banalysis\b",
]

TEAM_WORDS = {
    "diamondbacks", "braves", "orioles", "red sox", "cubs", "white sox", "reds",
    "guardians", "rockies", "tigers", "astros", "royals", "angels", "dodgers",
    "marlins", "brewers", "twins", "mets", "yankees", "athletics", "as",
    "phillies", "pirates", "padres", "giants", "mariners", "cardinals", "rays",
    "rangers", "blue jays", "nationals"
}

INJURY_WORDS = [
    "injured", "injury", "il", "day-to-day", "mri", "tightness", "soreness",
    "forearm", "elbow", "shoulder", "hamstring", "oblique", "back"
]
LINEUP_WORDS = [
    "lineup", "starting", "scratched", "batting", "leadoff", "cleanup", "rest day"
]
CLOSER_WORDS = [
    "closer", "save chance", "bullpen", "ninth inning"
]
CALLUP_WORDS = [
    "called up", "promoted", "recalled", "optioned", "sent down"
]
TRANSACTION_WORDS = [
    "traded", "signed", "released", "designated for assignment", "dfa",
    "selected the contract", "acquired", "activated", "reinstated"
]


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS posted_items (
            dedupe_key TEXT PRIMARY KEY,
            source_key TEXT NOT NULL,
            posted_at TEXT NOT NULL,
            title TEXT NOT NULL,
            link TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def was_posted(conn: sqlite3.Connection, dedupe_key: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM posted_items WHERE dedupe_key = ?",
        (dedupe_key,),
    ).fetchone()
    return row is not None


def mark_posted(
    conn: sqlite3.Connection,
    dedupe_key: str,
    source_key: str,
    title: str,
    link: str,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO posted_items
        (dedupe_key, source_key, posted_at, title, link)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            dedupe_key,
            source_key,
            datetime.now(UTC).isoformat(),
            title,
            link,
        ),
    )
    conn.commit()


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def strip_html(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return normalize_space(text)


def clean_title(title: str) -> str:
    title = html.unescape(title or "")
    title = normalize_space(title)
    title = re.sub(r"\s*[-–|]\s*(rotowire|mlb trade rumors).*?$", "", title, flags=re.I)
    return normalize_space(title)


def normalize_for_dedupe(text: str) -> str:
    text = clean_title(text).lower()
    replacements = {
        "injured list": "il",
        "10-day injured list": "10-day il",
        "15-day injured list": "15-day il",
        "60-day injured list": "60-day il",
        "designated for assignment": "dfa",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    return normalize_space(text)


def make_dedupe_key(player_name: str, title: str) -> str:
    raw = f"{player_name.lower()}||{normalize_for_dedupe(title)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def extract_player_name(text: str) -> str | None:
    text = normalize_space(text)

    patterns = [
        r"\b([A-Z][a-z]+(?:[-'][A-Z]?[a-z]+)?\s+[A-Z][a-z]+(?:[-'][A-Z]?[a-z]+)?(?:\s+(?:Jr\.|Sr\.|II|III|IV))?)\b",
        r"\b([A-Z][a-z]+(?:[-'][A-Z]?[a-z]+)?\s+[A-Z]\.\s+[A-Z][a-z]+(?:[-'][A-Z]?[a-z]+)?)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return normalize_space(match.group(1))
    return None


def contains_player_news_keyword(text: str) -> bool:
    t = text.lower()
    return any(keyword in t for keyword in PLAYER_NEWS_KEYWORDS)


def looks_like_article_or_team_news(title: str, summary: str) -> bool:
    combined = f"{title} {summary}".lower()

    for pattern in ARTICLE_PATTERNS:
        if re.search(pattern, combined, flags=re.I):
            return True

    has_team_word = any(team in combined for team in TEAM_WORDS)
    has_player = extract_player_name(title) or extract_player_name(summary)
    if has_team_word and not has_player:
        return True

    return False


def classify_news(text: str) -> tuple[str, str]:
    t = text.lower()

    if any(word in t for word in INJURY_WORDS):
        return "🚑", "Injury"
    if any(word in t for word in LINEUP_WORDS):
        return "🔄", "Lineup"
    if any(word in t for word in CLOSER_WORDS):
        return "🔒", "Bullpen"
    if any(word in t for word in CALLUP_WORDS):
        return "⬆️", "Call-Up"
    if any(word in t for word in TRANSACTION_WORDS):
        return "🚨", "Transaction"
    return "📰", "Player News"


def color_for_tag(tag: str) -> int:
    return {
        "Injury": 0xE74C3C,
        "Lineup": 0x3498DB,
        "Bullpen": 0x9B59B6,
        "Call-Up": 0x2ECC71,
        "Transaction": 0xF39C12,
        "Player News": 0x95A5A6,
    }.get(tag, 0x95A5A6)


def safe_text(text: str, limit: int) -> str:
    text = normalize_space(text)
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def fetch_feed(source: dict) -> list[dict]:
    parsed = feedparser.parse(source["url"])
    items = []

    for entry in parsed.entries[:25]:
        title = clean_title(getattr(entry, "title", ""))
        link = normalize_space(getattr(entry, "link", ""))
        summary = strip_html(getattr(entry, "summary", ""))

        items.append(
            {
                "source_name": source["name"],
                "source_key": source["key"],
                "priority": source["priority"],
                "title": title,
                "link": link,
                "summary": summary,
            }
        )

    return items


def is_valid_player_news_item(item: dict) -> tuple[bool, str | None]:
    title = item["title"]
    summary = item["summary"]
    combined = f"{title} {summary}"

    if looks_like_article_or_team_news(title, summary):
        return False, None

    player_name = extract_player_name(title) or extract_player_name(summary)
    if not player_name:
        return False, None

    if not contains_player_news_keyword(combined):
        return False, None

    return True, player_name


def choose_best_items(items: list[dict]) -> list[dict]:
    chosen: dict[str, dict] = {}

    for item in items:
        ok, player_name = is_valid_player_news_item(item)
        if not ok or not player_name:
            continue

        item["player_name"] = player_name
        dedupe_key = make_dedupe_key(player_name, item["title"])
        item["dedupe_key"] = dedupe_key

        if dedupe_key not in chosen:
            chosen[dedupe_key] = item
            continue

        current = chosen[dedupe_key]
        if item["priority"] < current["priority"]:
            chosen[dedupe_key] = item

    final_items = list(chosen.values())
    final_items.sort(key=lambda x: (x["priority"], x["title"].lower()))
    return final_items


def post_to_discord(item: dict) -> None:
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL is not set")

    combined = f"{item['title']} {item['summary']}"
    emoji, tag = classify_news(combined)

    description = f"**{item['title']}**"
    if item["summary"] and item["summary"].lower() != item["title"].lower():
        description += f"\n\n{safe_text(item['summary'], 1200)}"

    payload = {
        "username": "MLB Player News",
        "embeds": [
            {
                "title": f"{emoji} {item['player_name']}",
                "url": item["link"],
                "description": description,
                "color": color_for_tag(tag),
                "fields": [
                    {"name": "Tag", "value": tag, "inline": True},
                    {"name": "Source", "value": item["source_name"], "inline": True},
                ],
                "footer": {"text": "RSS player news"},
                "timestamp": datetime.now(UTC).isoformat(),
            }
        ],
        "allowed_mentions": {"parse": []},
    }

    response = requests.post(
        DISCORD_WEBHOOK_URL,
        json=payload,
        timeout=HTTP_TIMEOUT,
    )
    response.raise_for_status()


def main() -> None:
    conn = init_db()

    raw_items: list[dict] = []
    for source in FEEDS:
        try:
            raw_items.extend(fetch_feed(source))
        except Exception as exc:
            print(f"Feed failed for {source['name']}: {exc}")

    final_items = choose_best_items(raw_items)
    print(f"Eligible player-news items found: {len(final_items)}")

    posted = 0
    for item in final_items:
        if was_posted(conn, item["dedupe_key"]):
            continue

        try:
            post_to_discord(item)
            mark_posted(
                conn,
                item["dedupe_key"],
                item["source_key"],
                item["title"],
                item["link"],
            )
            posted += 1
            time.sleep(1)
        except Exception as exc:
            print(f"Failed posting {item['title']}: {exc}")

        if posted >= MAX_POSTS_PER_RUN:
            break

    print(f"Posted {posted}")


if __name__ == "__main__":
    main()
