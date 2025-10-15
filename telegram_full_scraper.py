#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
telegram_web_scraper_cli.py

Web-only Telegram scraper (NO API KEY, NO WEB UI).
- Scrapes public channel web view: https://t.me/s/<channel>
- Optional: scrape a public discussion group and map comments -> channel posts (heuristics)
- Stores into SQLite with FTS5; indexes posts + comments
- Media capture (best-effort)
- ETag/Last-Modified caching for polite, faster resumes
- Combined search (posts + comments) with filters
- CSV/JSONL export

Install:
  pip install requests beautifulsoup4 python-dotenv

Quick start:
  # scrape a channel
  python telegram_web_scraper_cli.py scrape --channel somechannel --db data.db
  # (optional) scrape a public discussion group to map comments
  python telegram_web_scraper_cli.py scrape --channel somechannel --discussion somegroup --db data.db
  # search (combined posts+comments)
  python telegram_web_scraper_cli.py search --db data.db --query '"exact phrase" OR keyword' --limit 50
  # export CSV/JSONL with filters
  python telegram_web_scraper_cli.py export --db data.db --query drone --kind post --out results.csv --format csv
"""

import argparse
import csv
import io
import json
import math
import os
import random
import re
import sqlite3
import sys
import time
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ------------------------ Config ------------------------

BASE = "https://t.me"
SBASE = "https://t.me/s"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}
REQUEST_TIMEOUT = 20
INITIAL_DELAY = 1.0
MAX_DELAY = 90.0
SMALL_SLEEP = 0.5

SELECTORS = {
    "wrap": "div.tgme_widget_message_wrap",
    "msg": "div.tgme_widget_message",
    "text": [".tgme_widget_message_text", ".js-message_text"],
    "desc": [".tgme_widget_message_description"],
    "time": "time",
    "views": ".tgme_widget_message_views",
    "forwarded": ".tgme_widget_message_forwarded_from",
    "reply": ".tgme_widget_message_reply",
    # Media-ish areas (best-effort)
    "photo_wrap": ".tgme_widget_message_photo_wrap",
    "video_wrap": ".tgme_widget_message_video",
    "link_anchor": ".tgme_widget_message_link_preview a",
}

# ------------------------ SQLite schema & migrations ------------------------

SCHEMA_V1 = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS channel_posts (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_username TEXT,
    post_id INTEGER,
    date_ts INTEGER,
    text TEXT,
    views INTEGER,
    permalink TEXT,
    has_media INTEGER DEFAULT 0,
    content_hash TEXT,
    UNIQUE(channel_username, post_id)
);

CREATE TABLE IF NOT EXISTS post_media (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_rowid INTEGER,
    kind TEXT,  -- image|video|link
    url TEXT
);

CREATE TABLE IF NOT EXISTS group_comments (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    group_username TEXT,
    group_msg_id INTEGER,
    date_ts INTEGER,
    text TEXT,
    reply_to_channel_username TEXT,
    reply_to_channel_msg_id INTEGER,
    permalink TEXT,
    map_conf REAL DEFAULT 0.0,
    UNIQUE(group_username, group_msg_id)
);

-- FTS tables
CREATE VIRTUAL TABLE IF NOT EXISTS posts_fts USING fts5(text, content_rowid UNINDEXED);
CREATE VIRTUAL TABLE IF NOT EXISTS comments_fts USING fts5(text, content_rowid UNINDEXED);

-- Simple cache for conditional requests
CREATE TABLE IF NOT EXISTS http_cache (
    url TEXT PRIMARY KEY,
    etag TEXT,
    last_modified TEXT,
    last_status INTEGER,
    last_fetched_ts INTEGER
);

PRAGMA user_version = 1;
"""

def ensure_db(path: str):
    con = sqlite3.connect(path)
    try:
        uv = con.execute("PRAGMA user_version").fetchone()[0]
        if uv == 0:
            con.executescript(SCHEMA_V1)
            con.commit()
        elif uv == 1:
            pass
        else:
            pass  # future migrations would go here
    finally:
        con.close()

# ------------------------ Helpers ------------------------

def normalize_handle(handle: str) -> str:
    h = handle.strip()
    h = re.sub(r'^https?://(www\.)?t\.me/(s/)?', '', h)
    h = h.lstrip('@').strip('/')
    return h

def first_text(soup: BeautifulSoup, choices: List[str]) -> str:
    for sel in choices:
        node = soup.select_one(sel)
        if node:
            t = node.get_text("\n", strip=True)
            if t:
                return t
    return ""

def content_hash(text: Optional[str]) -> str:
    import hashlib
    return hashlib.sha1((text or "").encode("utf-8", "ignore")).hexdigest()

def full_jitter_wait(base: float, retries: int) -> float:
    wait = min(MAX_DELAY, base * (2 ** max(0, retries)))
    return random.uniform(wait * 0.5, wait)

def tsfmt(ts: Optional[int]) -> str:
    if not ts:
        return ""
    try:
        return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return ""

# ------------------------ HTTP with caching ------------------------

def get_cache(con: sqlite3.Connection, url: str) -> Tuple[Optional[str], Optional[str]]:
    row = con.execute("SELECT etag, last_modified FROM http_cache WHERE url = ?", (url,)).fetchone()
    return (row[0], row[1]) if row else (None, None)

def set_cache(con: sqlite3.Connection, url: str, etag: Optional[str], last_modified: Optional[str], status: int):
    con.execute(
        "INSERT OR REPLACE INTO http_cache(url, etag, last_modified, last_status, last_fetched_ts) VALUES(?,?,?,?,?)",
        (url, etag, last_modified, status, int(time.time()))
    )
    con.commit()

def http_get(url: str, session: requests.Session, con: sqlite3.Connection,
             delay_base: float = INITIAL_DELAY) -> Optional[requests.Response]:
    et, lm = get_cache(con, url)
    headers = dict(HEADERS)
    if et:
        headers["If-None-Match"] = et
    if lm:
        headers["If-Modified-Since"] = lm

    retries = 0
    while True:
        try:
            resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        except requests.RequestException:
            if retries > 6:
                return None
            time.sleep(full_jitter_wait(delay_base, retries)); retries += 1
            continue

        if resp.status_code == 200:
            set_cache(con, url, resp.headers.get("ETag"), resp.headers.get("Last-Modified"), 200)
            return resp
        if resp.status_code == 304:
            set_cache(con, url, et, lm, 304)
            return resp
        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            wait = int(ra) if ra and ra.isdigit() else max(5, full_jitter_wait(delay_base, retries))
            time.sleep(wait); retries += 1
            continue
        if 500 <= resp.status_code < 600:
            if retries > 6:
                set_cache(con, url, None, None, resp.status_code)
                return resp
            time.sleep(full_jitter_wait(delay_base, retries)); retries += 1
            continue
        set_cache(con, url, None, None, resp.status_code)
        return resp

# ------------------------ Parsing ------------------------

def parse_channel_page(html: str) -> Tuple[List[dict], Optional[int], List[Tuple[str, str]]]:
    """
    Returns (posts, before_id, media_entries)
    media_entries: list of (kind, url) pairs for *all* posts on the page (we reattach by rowid after insert)
    """
    soup = BeautifulSoup(html, "html.parser")
    posts = []
    media_entries = []

    for wrap in soup.select(SELECTORS["wrap"]):
        x = wrap.select_one(SELECTORS["msg"])
        if not x:
            continue

        data_post = x.get("data-post")  # like 'channel/1234'
        if not data_post or "/" not in data_post:
            continue
        ch, pid = data_post.split("/", 1)
        try:
            post_id = int(pid)
        except ValueError:
            continue
        permalink = f"{BASE}/{data_post}"

        # Time
        dt = None
        t = x.select_one(SELECTORS["time"])
        if t and t.has_attr("datetime"):
            try:
                dt = int(datetime.fromisoformat(t["datetime"].replace("Z", "+00:00")).timestamp())
            except Exception:
                dt = None

        # Text
        text = "\n\n".join(filter(None, [
            first_text(x, SELECTORS["text"]),
            first_text(x, SELECTORS["desc"])
        ]))

        # Views
        views = None
        v = x.select_one(SELECTORS["views"])
        if v:
            digits = re.sub(r"[^\d]", "", v.get_text())
            views = int(digits) if digits.isdigit() else None

        # Media (best-effort)
        has_media = 0
        pwrap = x.select_one(SELECTORS["photo_wrap"])
        if pwrap:
            style = pwrap.get("style") or ""
            m = re.search(r"url\(['\"]?(.*?)['\"]?\)", style)
            if m:
                media_entries.append(("image", m.group(1)))
                has_media = 1
            for img in pwrap.select("img"):
                src = img.get("src")
                if src:
                    media_entries.append(("image", src)); has_media = 1

        vwrap = x.select_one(SELECTORS["video_wrap"])
        if vwrap:
            for a in vwrap.select("a"):
                href = a.get("href")
                if href:
                    media_entries.append(("video", href)); has_media = 1

        for a in x.select(SELECTORS["link_anchor"]):
            href = a.get("href")
            if href:
                media_entries.append(("link", href)); has_media = 1

        posts.append({
            "channel_username": ch,
            "post_id": post_id,
            "date_ts": dt,
            "text": text,
            "views": views,
            "permalink": permalink,
            "has_media": has_media,
            "content_hash": content_hash(text),
        })

    # Pagination: choose the *smallest* before= on page to ensure progress
    before_id = None
    for a in soup.select("a[href*='?before=']"):
        href = a.get("href") or ""
        try:
            bid = int(href.split("?before=")[-1].split("&")[0])
        except Exception:
            continue
        if before_id is None or bid < before_id:
            before_id = bid

    return posts, before_id, media_entries

def parse_group_for_mapping(html: str, channel_username: str) -> List[dict]:
    """
    Heuristics for mapping comments to channel posts:
    - Forwarded-from blocks linking to https://t.me/<channel>/<id>
    - Reply blocks containing anchor to channel post
    """
    soup = BeautifulSoup(html, "html.parser")
    out = []

    for wrap in soup.select(SELECTORS["wrap"]):
        x = wrap.select_one(SELECTORS["msg"])
        if not x:
            continue

        data_post = x.get("data-post")
        if not data_post or "/" not in data_post:
            continue
        gp, mid = data_post.split("/", 1)
        try:
            group_msg_id = int(mid)
        except ValueError:
            continue
        permalink = f"{BASE}/{data_post}"

        # time
        dt = None
        t = x.select_one(SELECTORS["time"])
        if t and t.has_attr("datetime"):
            try:
                dt = int(datetime.fromisoformat(t["datetime"].replace("Z", "+00:00")).timestamp())
            except Exception:
                dt = None

        # text
        text = first_text(x, SELECTORS["text"])

        # anchors we can parse
        reply_to_channel_msg_id = None
        fwd = x.select_one(SELECTORS["forwarded"])
        if fwd:
            for a in fwd.select("a"):
                href = a.get("href") or ""
                m = re.match(rf"^https://t\.me/{re.escape(channel_username)}/(\d+)$", href)
                if m:
                    try:
                        reply_to_channel_msg_id = int(m.group(1)); break
                    except Exception:
                        pass
        if reply_to_channel_msg_id is None:
            rblock = x.select_one(SELECTORS["reply"])
            if rblock:
                for a in rblock.select("a"):
                    href = a.get("href") or ""
                    m = re.match(rf"^https://t\.me/{re.escape(channel_username)}/(\d+)$", href)
                    if m:
                        try:
                            reply_to_channel_msg_id = int(m.group(1)); break
                        except Exception:
                            pass

        out.append({
            "group_username": gp,
            "group_msg_id": group_msg_id,
            "date_ts": dt,
            "text": text,
            "permalink": permalink,
            "reply_to_channel_username": channel_username,
            "reply_to_channel_msg_id": reply_to_channel_msg_id,
        })
    return out

# ------------------------ DB write ops ------------------------

def upsert_channel_posts(con: sqlite3.Connection, posts: List[dict]):
    cur = con.cursor()
    for p in posts:
        cur.execute(
            """INSERT OR IGNORE INTO channel_posts
               (channel_username, post_id, date_ts, text, views, permalink, has_media, content_hash)
               VALUES(?,?,?,?,?,?,?,?)""",
            (p["channel_username"], p["post_id"], p["date_ts"], p["text"], p["views"],
             p["permalink"], p["has_media"], p["content_hash"])
        )
        r = cur.execute(
            "SELECT rowid FROM channel_posts WHERE channel_username=? AND post_id=?",
            (p["channel_username"], p["post_id"])
        ).fetchone()
        if r:
            rowid = r[0]
            cur.execute("INSERT OR REPLACE INTO posts_fts(rowid, text) VALUES(?,?)", (rowid, p["text"] or ""))
    con.commit()

def attach_media_to_last_inserted(con: sqlite3.Connection, channel_username: str, page_posts: List[dict], media_entries: List[Tuple[str, str]]):
    if not media_entries or not page_posts:
        return
    cur = con.cursor()
    ids = []
    for p in page_posts:
        r = cur.execute(
            "SELECT rowid FROM channel_posts WHERE channel_username=? AND post_id=?",
            (p["channel_username"], p["post_id"])
        ).fetchone()
        if r:
            ids.append(r[0])
    if not ids:
        return
    i = 0
    for kind, url in media_entries:
        rid = ids[i % len(ids)]
        cur.execute("INSERT INTO post_media(post_rowid, kind, url) VALUES(?,?,?)", (rid, kind, url))
        cur.execute("UPDATE channel_posts SET has_media=1 WHERE rowid=?", (rid,))
        i += 1
    con.commit()

def upsert_group_comments(con: sqlite3.Connection, comments: List[dict]):
    cur = con.cursor()
    for c in comments:
        cur.execute(
            """INSERT OR IGNORE INTO group_comments
               (group_username, group_msg_id, date_ts, text, reply_to_channel_username,
                reply_to_channel_msg_id, permalink, map_conf)
               VALUES(?,?,?,?,?,?,?,?)""",
            (c["group_username"], c["group_msg_id"], c["date_ts"], c["text"],
             c["reply_to_channel_username"], c["reply_to_channel_msg_id"],
             c["permalink"], 0.0)
        )
        r = cur.execute(
            "SELECT rowid FROM group_comments WHERE group_username=? AND group_msg_id=?",
            (c["group_username"], c["group_msg_id"])
        ).fetchone()
        if r:
            rowid = r[0]
            cur.execute("INSERT OR REPLACE INTO comments_fts(rowid, text) VALUES(?,?)", (rowid, c["text"] or ""))
    con.commit()

def jaccard_conf(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    ta = set(re.findall(r"\w+", a.lower()))
    tb = set(re.findall(r"\w+", b.lower()))
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)

def compute_mapping_confidence(con: sqlite3.Connection, channel_username: str, window: int = 72*3600):
    cur = con.cursor()
    rows = cur.execute("""
        SELECT rowid, text, date_ts, reply_to_channel_msg_id
        FROM group_comments
        WHERE reply_to_channel_username=?""", (channel_username,)).fetchall()

    for row in rows:
        rid, ctext, cts, refid = row
        conf = 0.0
        if refid:
            p = cur.execute("""
                SELECT text FROM channel_posts WHERE channel_username=? AND post_id=?""",
                (channel_username, refid)
            ).fetchone()
            if p:
                conf = jaccard_conf(ctext or "", p[0] or "")
        if conf == 0.0 and cts:
            p2 = cur.execute("""
                SELECT text FROM channel_posts
                WHERE channel_username=? AND ABS(date_ts - ?) <= ?
                ORDER BY ABS(date_ts - ?) ASC LIMIT 3
            """, (channel_username, cts or 0, window, cts or 0)).fetchall()
            for (ptxt,) in p2:
                conf = max(conf, jaccard_conf(ctext or "", ptxt or ""))
        cur.execute("UPDATE group_comments SET map_conf=? WHERE rowid=?", (conf, rid))
    con.commit()

# ------------------------ Scraping flows ------------------------

def scrape_channel(db: str, channel: str):
    ensure_db(db)
    ch = normalize_handle(channel)
    sess = requests.Session()
    con = sqlite3.connect(db)

    url = f"{SBASE}/{ch}"
    seen = 0
    visited_before_values = set()
    while True:
        resp = http_get(url, sess, con)
        if resp is None:
            print(f"Failed to fetch {url}")
            break
        if resp.status_code == 304:
            html = resp.text or ""
        elif resp.status_code == 404:
            print(f"Channel not found or not public: {ch}")
            break
        elif resp.status_code != 200:
            print(f"HTTP {resp.status_code} for {url}")
            break
        else:
            html = resp.text

        posts, before_id, media_entries = parse_channel_page(html)
        if posts:
            upsert_channel_posts(con, posts)
            attach_media_to_last_inserted(con, ch, posts, media_entries)
            seen += len(posts)
            print(f"[channel:{ch}] saved {len(posts)} (total {seen})")

        if before_id is None or before_id in visited_before_values:
            break
        visited_before_values.add(before_id)
        url = f"{SBASE}/{ch}?before={before_id}"
        time.sleep(SMALL_SLEEP)
    con.close()

def scrape_group(db: str, group: str, channel_for_mapping: str):
    ensure_db(db)
    gp = normalize_handle(group)
    ch = normalize_handle(channel_for_mapping)
    sess = requests.Session()
    con = sqlite3.connect(db)

    url = f"{SBASE}/{gp}"
    seen = 0
    visited_before_values = set()
    while True:
        resp = http_get(url, sess, con)
        if resp is None:
            print(f"Failed to fetch {url}")
            break
        if resp.status_code == 304:
            html = resp.text or ""
        elif resp.status_code == 404:
            print(f"Group not found or not public: {gp}")
            break
        elif resp.status_code != 200:
            print(f"HTTP {resp.status_code} for {url}")
            break
        else:
            html = resp.text

        comments = parse_group_for_mapping(html, ch)
        if comments:
            upsert_group_comments(con, comments)
            seen += len(comments)
            print(f"[group:{gp}] mapped {len(comments)} (total {seen})")

        soup = BeautifulSoup(html, "html.parser")
        before_id = None
        for a in soup.select("a[href*='?before=']"):
            href = a.get("href") or ""
            try:
                bid = int(href.split("?before=")[-1].split("&")[0])
            except Exception:
                continue
            if before_id is None or bid < before_id:
                before_id = bid
        if before_id is None or before_id in visited_before_values:
            break
        visited_before_values.add(before_id)
        url = f"{SBASE}/{gp}?before={before_id}"
        time.sleep(SMALL_SLEEP)

    compute_mapping_confidence(con, ch)
    con.close()

# ------------------------ Search & Export ------------------------

def search_all(con: sqlite3.Connection, q: str, kind: Optional[str] = None,
               channel: Optional[str] = None, group: Optional[str] = None,
               has_media: Optional[bool] = None,
               date_from: Optional[int] = None, date_to: Optional[int] = None,
               limit: int = 200, offset: int = 0) -> List[sqlite3.Row]:
    """
    Combined search across posts_fts and comments_fts, with filters.
    kind: 'post'|'comment'|None (both)
    """
    con.row_factory = sqlite3.Row
    params: List[object] = []
    blocks = []

    if kind in (None, 'post'):
        sqlp = """
          SELECT 'post' AS kind, p.rowid, p.channel_username AS owner, p.post_id AS mid,
                 p.date_ts, p.text, p.permalink, p.has_media, NULL AS map_conf
          FROM posts_fts f
          JOIN channel_posts p ON p.rowid = f.rowid
          WHERE posts_fts MATCH ?
        """
        params.append(q)
        if channel:
            sqlp += " AND p.channel_username = ?"
            params.append(channel)
        if has_media is True:
            sqlp += " AND p.has_media = 1"
        if has_media is False:
            sqlp += " AND p.has_media = 0"
        if date_from is not None:
            sqlp += " AND p.date_ts >= ?"; params.append(date_from)
        if date_to is not None:
            sqlp += " AND p.date_ts <= ?"; params.append(date_to)
        blocks.append(sqlp)

    if kind in (None, 'comment'):
        sqlc = """
          SELECT 'comment' AS kind, c.rowid, c.group_username AS owner, c.group_msg_id AS mid,
                 c.date_ts, c.text, c.permalink, NULL AS has_media, c.map_conf
          FROM comments_fts f
          JOIN group_comments c ON c.rowid = f.rowid
          WHERE comments_fts MATCH ?
        """
        params.append(q)
        if group:
            sqlc += " AND c.group_username = ?"; params.append(group)
        if date_from is not None:
            sqlc += " AND c.date_ts >= ?"; params.append(date_from)
        if date_to is not None:
            sqlc += " AND c.date_ts <= ?"; params.append(date_to)
        blocks.append(sqlc)

    if not blocks:
        return []

    final = " UNION ALL ".join(blocks) + " ORDER BY date_ts DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    return con.execute(final, params).fetchall()

def write_csv(rows: List[sqlite3.Row], out: Optional[str]):
    if not rows:
        if out:
            with open(out, "w", newline="", encoding="utf-8") as f:
                f.write("")
        return
    header = list(rows[0].keys())
    if out:
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            for r in rows:
                w.writerow([r[h] for h in header])
    else:
        w = csv.writer(sys.stdout)
        w.writerow(header)
        for r in rows:
            w.writerow([r[h] for h in header])

def write_jsonl(rows: List[sqlite3.Row], out: Optional[str]):
    def gen():
        for r in rows:
            yield json.dumps(dict(r), ensure_ascii=False) + "\n"
    if out:
        with open(out, "w", encoding="utf-8") as f:
            for line in gen():
                f.write(line)
    else:
        for line in gen():
            sys.stdout.write(line)

# ------------------------ CLI ------------------------

def cmd_scrape(args):
    if args.channel:
        scrape_channel(args.db, args.channel)
    if args.discussion:
        scrape_group(args.db, args.discussion, args.channel or "")

def cmd_search(args):
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    # convert date filters YYYY-MM-DD -> ts
    def to_ts(s: Optional[str]) -> Optional[int]:
        if not s: return None
        try: return int(datetime.fromisoformat(s).replace(tzinfo=None).timestamp())
        except Exception: return None
    dfrom = to_ts(args.date_from)
    dto = to_ts(args.date_to)

    rows = search_all(
        con, args.query, kind=args.kind, channel=args.channel, group=args.group,
        has_media=(True if args.has_media == "1" else False if args.has_media == "0" else None),
        date_from=dfrom, date_to=dto, limit=args.limit, offset=args.offset
    )
    for r in rows:
        print(f"[{r['kind']}] {r['owner']} #{r['mid']} @ {tsfmt(r['date_ts'])} "
              f"{'(media)' if r['kind']=='post' and r['has_media'] else ''}"
              f"{(' conf=' + str(round(r['map_conf'],2))) if r['kind']=='comment' else ''}")
        print((r['text'] or "")[:500])
        print(r['permalink'])
        print("-"*60)
    con.close()

def cmd_export(args):
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    def to_ts(s: Optional[str]) -> Optional[int]:
        if not s: return None
        try: return int(datetime.fromisoformat(s).replace(tzinfo=None).timestamp())
        except Exception: return None
    dfrom = to_ts(args.date_from)
    dto = to_ts(args.date_to)
    rows = search_all(
        con, args.query or "*", kind=args.kind, channel=args.channel, group=args.group,
        has_media=(True if args.has_media == "1" else False if args.has_media == "0" else None),
        date_from=dfrom, date_to=dto, limit=args.limit, offset=args.offset
    )
    if args.format == "jsonl":
        write_jsonl(rows, args.out)
    else:
        write_csv(rows, args.out)
    con.close()

def main():
    ap = argparse.ArgumentParser(prog="telegram_web_scraper_cli")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("scrape", help="Scrape public channel and optional public discussion group")
    s.add_argument("--db", default="telegram_web.db")
    s.add_argument("--channel", help="Public channel handle (e.g., @foo or https://t.me/foo)")
    s.add_argument("--discussion", help="Public discussion group handle for mapping comments")
    s.set_defaults(func=cmd_scrape)

    sc = sub.add_parser("search", help="CLI full-text search across posts+comments")
    sc.add_argument("--db", default="telegram_web.db")
    sc.add_argument("--query", required=True, help='FTS5 query: words, "exact phrase", foo OR bar')
    sc.add_argument("--kind", choices=["post","comment"], default=None)
    sc.add_argument("--channel", default=None, help="Filter to a channel handle")
    sc.add_argument("--group", default=None, help="Filter to a group handle")
    sc.add_argument("--has-media", dest="has_media", choices=["","0","1"], default="", help="Posts: 1=yes, 0=no, ''=any")
    sc.add_argument("--date-from", default=None, help="YYYY-MM-DD (UTC)")
    sc.add_argument("--date-to", default=None, help="YYYY-MM-DD (UTC)")
    sc.add_argument("--limit", type=int, default=100)
    sc.add_argument("--offset", type=int, default=0)
    sc.set_defaults(func=cmd_search)

    ex = sub.add_parser("export", help="Export CSV/JSONL of a search")
    ex.add_argument("--db", default="telegram_web.db")
    ex.add_argument("--query", default="*", help='FTS5 query; default "*"')
    ex.add_argument("--kind", choices=["post","comment"], default=None)
    ex.add_argument("--channel", default=None)
    ex.add_argument("--group", default=None)
    ex.add_argument("--has-media", dest="has_media", choices=["","0","1"], default="")
    ex.add_argument("--date-from", default=None, help="YYYY-MM-DD")
    ex.add_argument("--date-to", default=None, help="YYYY-MM-DD")
    ex.add_argument("--limit", type=int, default=100000)
    ex.add_argument("--offset", type=int, default=0)
    ex.add_argument("--format", choices=["csv","jsonl"], default="csv")
    ex.add_argument("--out", default=None, help="Output file path (omit to write to stdout)")
    ex.set_defaults(func=cmd_export)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
