#!/usr/bin/env python3
"""
Qullamaggie Stream Harvester v2

What it does
------------
- Collects channel videos via yt-dlp or YouTube Data API
- Filters for likely livestreams if requested
- Fetches transcripts via youtube-transcript-api
- Falls back to yt-dlp subtitle download when transcript API fails
- Parses subtitle formats: transcript-api JSON, VTT, SRT, JSON3
- Extracts:
  - five-star / 5-star mentions
  - ticker mentions
  - likely setup type: EP / Breakout / Parabolic Short / Other
  - timestamp, context windows, source confidence
- Produces CSV / JSON / markdown reports
- Optionally downloads and validates tickers against SEC company tickers
- Designed to run locally or on GitHub Actions
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import time
import shutil
import tempfile
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from youtube_transcript_api import YouTubeTranscriptApi
except Exception:
    YouTubeTranscriptApi = None

try:
    from yt_dlp import YoutubeDL
except Exception:
    YoutubeDL = None

try:
    import requests
except Exception:
    requests = None


DEFAULT_CHANNEL_URL = "https://www.youtube.com/@qullamaggie/videos"

FIVE_STAR_PATTERNS = [
    re.compile(r"\b5[\s\-]*star\b", re.I),
    re.compile(r"\bfive[\s\-]*star\b", re.I),
    re.compile(r"\b5[\s\-]*star[\s\-]*plus\b", re.I),
    re.compile(r"\bfive[\s\-]*star[\s\-]*plus\b", re.I),
]

SETUP_PATTERNS = {
    "EP": [
        re.compile(r"\bearnings[\s\-]*pivot\b", re.I),
        re.compile(r"\bearnings[\s\-]*gap\b", re.I),
        re.compile(r"\bEP\b"),
        re.compile(r"\bpost[\s\-]*earnings\b", re.I),
    ],
    "BREAKOUT": [
        re.compile(r"\bbreakout\b", re.I),
        re.compile(r"\bopening[\s\-]*range\b", re.I),
        re.compile(r"\bORB\b"),
        re.compile(r"\btight\b", re.I),
        re.compile(r"\bconsolidat", re.I),
    ],
    "PARABOLIC_SHORT": [
        re.compile(r"\bparabolic[\s\-]*short\b", re.I),
        re.compile(r"\bblow[\s\-]*off\b", re.I),
        re.compile(r"\bclimactic\b", re.I),
        re.compile(r"\bshort\b", re.I),
    ],
}

STYLE_KEYWORDS = {
    "setup": ["earnings", "ep", "breakout", "parabolic short", "gap up", "opening range", "orb", "ema10", "ema 10", "ema20", "sma50", "inside day", "consolidation", "tight", "relative strength", "leader"],
    "risk": ["stop", "risk", "position size", "sizing", "trim", "sell into strength", "trailing stop", "cut loss"],
    "market": ["market environment", "follow through", "breadth", "leading stocks", "indices", "regime"],
}

TICKER_RE = re.compile(r"\b[A-Z]{1,5}\b")
COMMON_BAD_TICKERS = {
    "I","A","AI","TV","US","USA","GDP","CPI","FED","FOMC","CEO","IPO","ATM","IMO","FYI",
    "FAQ","USD","EUR","GBP","CAD","AUD","JPY","OTC","SEC","ETF","ETFS","EPS","PE","PNL",
    "RSI","MACD","DMA","EMA","SMA","VWAP","OR","D","W","M","T","Y","Q","K","OK","LIVE",
    "NEWS","LONG","SHORT","BID","ASK","LOL","ATH","ATL","HOD","LOD","THE","AND","FOR","NOT",
    "YES","NO","YOU","WE","HE","SHE","IT","ARE","WAS","HIS","HER","THIS","THAT"
}


@dataclass
class VideoMeta:
    video_id: str
    title: str
    url: str
    upload_date: Optional[str] = None
    duration: Optional[int] = None
    channel: Optional[str] = None
    is_live: Optional[bool] = None


@dataclass
class TranscriptStatus:
    video_id: str
    title: str
    url: str
    upload_date: Optional[str]
    transcript_ok: bool
    source: str
    note: str


@dataclass
class Hit:
    video_id: str
    title: str
    upload_date: Optional[str]
    url: str
    timestamp_hms: str
    start_seconds: float
    kind: str
    setup_type: str
    confidence: str
    match_text: str
    nearby_tickers: str
    validated_tickers: str
    context: str


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def hms(seconds: float) -> str:
    s = int(seconds)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}" if h else f"{m:02d}:{sec:02d}"


def sanitize_filename(name: str) -> str:
    name = re.sub(r"[^\w\-. ]+", "_", name).strip()
    return name[:180]


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def collect_videos_ytdlp(channel_url: str, live_only: bool = False, limit: Optional[int] = None) -> List[VideoMeta]:
    if YoutubeDL is None:
        raise RuntimeError("yt-dlp is not installed.")
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": True,
        "ignoreerrors": True,
        "playlistend": limit,
    }
    out = []
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(channel_url, download=False)
        entries = info.get("entries", []) if info else []
        for item in entries:
            if not item:
                continue
            title = item.get("title") or ""
            is_live = item.get("live_status") in {"is_live", "was_live", "post_live"} or "live" in title.lower() or "stream" in title.lower()
            if live_only and not is_live:
                continue
            vid = item.get("id")
            out.append(VideoMeta(
                video_id=vid,
                title=title,
                url=f"https://www.youtube.com/watch?v={vid}" if vid else item.get("url"),
                upload_date=item.get("upload_date"),
                duration=item.get("duration"),
                channel=item.get("channel") or item.get("uploader"),
                is_live=is_live,
            ))
    return out


def collect_videos_api(channel_id: str, api_key: str, live_only: bool = False, limit: Optional[int] = None) -> List[VideoMeta]:
    if requests is None:
        raise RuntimeError("requests is not installed.")
    ch_url = "https://www.googleapis.com/youtube/v3/channels"
    params = {"part": "contentDetails,snippet", "id": channel_id, "key": api_key}
    r = requests.get(ch_url, params=params, timeout=30)
    r.raise_for_status()
    items = r.json().get("items", [])
    if not items:
        raise RuntimeError("No channel found.")
    uploads = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
    out = []
    page = None
    while True:
        params = {
            "part": "snippet,contentDetails",
            "playlistId": uploads,
            "maxResults": 50,
            "key": api_key,
        }
        if page:
            params["pageToken"] = page
        r = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for item in data.get("items", []):
            sn = item["snippet"]
            title = sn.get("title") or ""
            is_live = "live" in title.lower() or "stream" in title.lower()
            if live_only and not is_live:
                continue
            vid = sn["resourceId"]["videoId"]
            out.append(VideoMeta(
                video_id=vid,
                title=title,
                url=f"https://www.youtube.com/watch?v={vid}",
                upload_date=(sn.get("publishedAt") or "")[:10],
                channel=sn.get("channelTitle"),
                is_live=is_live,
            ))
            if limit and len(out) >= limit:
                return out
        page = data.get("nextPageToken")
        if not page or (limit and len(out) >= limit):
            break
    return out


def fetch_transcript_api(video_id: str, languages: List[str]) -> Tuple[Optional[List[dict]], str]:
    if YouTubeTranscriptApi is None:
        return None, "youtube-transcript-api not installed"
    try:
        tx = YouTubeTranscriptApi.get_transcript(video_id, languages=languages)
        return tx, "ok"
    except Exception as e:
        return None, str(e)


def parse_vtt(text: str) -> List[dict]:
    out = []
    blocks = re.split(r"\n\s*\n", text.strip(), flags=re.M)
    for block in blocks:
        lines = [x.strip() for x in block.splitlines() if x.strip()]
        if not lines:
            continue
        if lines[0].upper().startswith("WEBVTT"):
            continue
        time_line = None
        for ln in lines:
            if "-->" in ln:
                time_line = ln
                break
        if not time_line:
            continue
        m = re.search(r"(\d{2}:)?\d{2}:\d{2}\.\d{3}\s*-->\s*(\d{2}:)?\d{2}:\d{2}\.\d{3}", time_line)
        if not m:
            continue
        parts = time_line.split("-->")
        start = to_seconds(parts[0].strip())
        payload = " ".join(ln for ln in lines if ln != time_line and not re.match(r"^\d+$", ln))
        payload = re.sub(r"<[^>]+>", "", payload).strip()
        if payload:
            out.append({"text": payload, "start": start})
    return out


def parse_srt(text: str) -> List[dict]:
    out = []
    blocks = re.split(r"\n\s*\n", text.strip(), flags=re.M)
    for block in blocks:
        lines = [x.rstrip() for x in block.splitlines() if x.strip()]
        if len(lines) < 2:
            continue
        time_line = None
        for ln in lines:
            if "-->" in ln:
                time_line = ln
                break
        if not time_line:
            continue
        parts = time_line.split("-->")
        start = to_seconds(parts[0].strip().replace(",", "."))
        payload = " ".join(ln for ln in lines if ln != time_line and not re.match(r"^\d+$", ln))
        payload = re.sub(r"<[^>]+>", "", payload).strip()
        if payload:
            out.append({"text": payload, "start": start})
    return out


def parse_json3(raw: dict) -> List[dict]:
    out = []
    events = raw.get("events", [])
    for ev in events:
        segs = ev.get("segs")
        if not segs:
            continue
        text = "".join(seg.get("utf8", "") for seg in segs).replace("\n", " ").strip()
        if not text:
            continue
        start = (ev.get("tStartMs") or 0) / 1000.0
        out.append({"text": text, "start": start})
    return out


def to_seconds(ts: str) -> float:
    ts = ts.strip()
    parts = ts.split(":")
    if len(parts) == 3:
        h, m, s = parts
    elif len(parts) == 2:
        h = 0
        m, s = parts
    else:
        return 0.0
    return float(h) * 3600 + float(m) * 60 + float(s)


def fetch_transcript_ytdlp(video_url: str, temp_dir: Path, languages: List[str]) -> Tuple[Optional[List[dict]], str]:
    if YoutubeDL is None:
        return None, "yt-dlp not installed"
    lang_candidates = languages + [x.split("-")[0] for x in languages if "-" in x]
    ydl_opts = {
        "skip_download": True,
        "quiet": True,
        "writesubtitles": True,
        "writeautomaticsub": True,
        "subtitleslangs": lang_candidates,
        "subtitlesformat": "vtt/srt/json3/best",
        "outtmpl": str(temp_dir / "%(id)s.%(ext)s"),
        "ignoreerrors": True,
    }
    try:
        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
    except Exception as e:
        return None, f"yt-dlp subtitle download failed: {e}"

    candidates = list(temp_dir.glob("*.*"))
    subtitle_files = [p for p in candidates if p.suffix.lower() in {".vtt", ".srt", ".json3"}]
    if not subtitle_files:
        return None, "no subtitle files written by yt-dlp"

    subtitle_files.sort(key=lambda p: (p.suffix.lower() != ".vtt", p.suffix.lower() != ".srt", p.name))
    for fp in subtitle_files:
        try:
            if fp.suffix.lower() == ".vtt":
                return parse_vtt(fp.read_text(encoding="utf-8", errors="ignore")), f"yt-dlp:{fp.name}"
            if fp.suffix.lower() == ".srt":
                return parse_srt(fp.read_text(encoding="utf-8", errors="ignore")), f"yt-dlp:{fp.name}"
            if fp.suffix.lower() == ".json3":
                return parse_json3(json.loads(fp.read_text(encoding="utf-8", errors="ignore"))), f"yt-dlp:{fp.name}"
        except Exception:
            continue
    return None, "subtitle parse failed"


def fetch_sec_tickers() -> Dict[str, dict]:
    if requests is None:
        return {}
    try:
        headers = {"User-Agent": "qulla-harvester/1.0 email@example.com"}
        r = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers, timeout=30)
        r.raise_for_status()
        raw = r.json()
        out = {}
        for _, row in raw.items():
            t = (row.get("ticker") or "").upper().strip()
            if t:
                out[t] = row
        return out
    except Exception:
        return {}


def extract_ticker_candidates(text: str) -> List[str]:
    vals = []
    for t in TICKER_RE.findall(text):
        if t in COMMON_BAD_TICKERS:
            continue
        if len(t) == 1:
            continue
        vals.append(t)
    seen = set()
    ordered = []
    for t in vals:
        if t not in seen:
            seen.add(t)
            ordered.append(t)
    return ordered


def classify_setup(text: str) -> str:
    hits = []
    for label, pats in SETUP_PATTERNS.items():
        score = sum(1 for p in pats if p.search(text))
        if score:
            hits.append((score, label))
    if not hits:
        return "OTHER"
    hits.sort(reverse=True)
    return hits[0][1]


def transcript_to_text(tx: List[dict]) -> str:
    return "\n".join(seg.get("text", "") for seg in tx)


def context_window(tx: List[dict], idx: int, radius: int = 3) -> str:
    lo = max(0, idx - radius)
    hi = min(len(tx), idx + radius + 1)
    return " ".join(seg.get("text", "").replace("\n", " ").strip() for seg in tx[lo:hi]).strip()


def analyze(video: VideoMeta, tx: List[dict], valid_tickers: Optional[Dict[str, dict]] = None) -> List[Hit]:
    rows: List[Hit] = []
    for i, seg in enumerate(tx):
        text = seg.get("text", "")
        start = float(seg.get("start", 0.0))
        ctx = context_window(tx, i, 3)
        nearby = extract_ticker_candidates(ctx)
        validated = [t for t in nearby if valid_tickers is None or t in valid_tickers]
        setup_type = classify_setup(ctx)

        matched = False
        for pat in FIVE_STAR_PATTERNS:
            m = pat.search(text)
            if m:
                rows.append(Hit(
                    video_id=video.video_id,
                    title=video.title,
                    upload_date=video.upload_date,
                    url=video.url,
                    timestamp_hms=hms(start),
                    start_seconds=start,
                    kind="five_star_mention",
                    setup_type=setup_type,
                    confidence="high" if validated else "medium",
                    match_text=m.group(0),
                    nearby_tickers=",".join(nearby),
                    validated_tickers=",".join(validated),
                    context=ctx,
                ))
                matched = True

        tickers = extract_ticker_candidates(text)
        if tickers and not matched:
            validated_seg = [t for t in tickers if valid_tickers is None or t in valid_tickers]
            rows.append(Hit(
                video_id=video.video_id,
                title=video.title,
                upload_date=video.upload_date,
                url=video.url,
                timestamp_hms=hms(start),
                start_seconds=start,
                kind="ticker_segment",
                setup_type=setup_type,
                confidence="medium" if validated_seg else "low",
                match_text=",".join(tickers[:15]),
                nearby_tickers=",".join(nearby),
                validated_tickers=",".join(validated_seg),
                context=ctx,
            ))
    return rows


def style_summary(text: str) -> Dict[str, int]:
    low = text.lower()
    return {bucket: sum(low.count(kw) for kw in kws) for bucket, kws in STYLE_KEYWORDS.items()}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel-url", default=DEFAULT_CHANNEL_URL)
    ap.add_argument("--mode", choices=["ytdlp", "api"], default="ytdlp")
    ap.add_argument("--channel-id", default=None)
    ap.add_argument("--api-key", default=os.getenv("YOUTUBE_API_KEY"))
    ap.add_argument("--live-only", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--langs", default="en,en-US")
    ap.add_argument("--sleep", type=float, default=0.2)
    ap.add_argument("--outdir", default="out")
    ap.add_argument("--save-all-transcripts", action="store_true")
    ap.add_argument("--validate-tickers", action="store_true")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    ensure_dir(outdir)
    ensure_dir(outdir / "transcripts")

    langs = [x.strip() for x in args.langs.split(",") if x.strip()]
    valid_tickers = fetch_sec_tickers() if args.validate_tickers else None

    if args.mode == "api":
        if not args.channel_id or not args.api_key:
            print("API mode requires --channel-id and --api-key", file=sys.stderr)
            return 2
        videos = collect_videos_api(args.channel_id, args.api_key, live_only=args.live_only, limit=args.limit)
    else:
        videos = collect_videos_ytdlp(args.channel_url, live_only=args.live_only, limit=args.limit)

    if not videos:
        print("No videos found.", file=sys.stderr)
        return 1

    write_csv(outdir / "videos.csv", [asdict(v) for v in videos],
              ["video_id", "title", "url", "upload_date", "duration", "channel", "is_live"])

    statuses: List[TranscriptStatus] = []
    hits: List[Hit] = []
    full_text_chunks: List[str] = []

    for idx, video in enumerate(videos, start=1):
        print(f"[{idx}/{len(videos)}] {video.title}")
        tx, note = fetch_transcript_api(video.video_id, langs)
        source = "youtube-transcript-api"
        if not tx:
            with tempfile.TemporaryDirectory() as td:
                tx, note = fetch_transcript_ytdlp(video.url, Path(td), langs)
                source = "yt-dlp-subtitles"

        ok = bool(tx)
        statuses.append(TranscriptStatus(
            video_id=video.video_id,
            title=video.title,
            url=video.url,
            upload_date=video.upload_date,
            transcript_ok=ok,
            source=source,
            note=note,
        ))

        if not ok:
            time.sleep(args.sleep)
            continue

        if args.save_all_transcripts:
            stem = sanitize_filename(f"{video.upload_date or 'unknown'}__{video.video_id}__{video.title}")
            (outdir / "transcripts" / f"{stem}.json").write_text(json.dumps(tx, ensure_ascii=False, indent=2), encoding="utf-8")
            (outdir / "transcripts" / f"{stem}.txt").write_text(transcript_to_text(tx), encoding="utf-8")

        local_hits = analyze(video, tx, valid_tickers=valid_tickers)
        hits.extend(local_hits)
        full_text_chunks.append(transcript_to_text(tx))
        time.sleep(args.sleep)

    write_csv(outdir / "transcript_status.csv", [asdict(s) for s in statuses],
              ["video_id", "title", "url", "upload_date", "transcript_ok", "source", "note"])

    hit_rows = [asdict(h) for h in hits]
    write_csv(outdir / "all_hits.csv", hit_rows,
              ["video_id", "title", "upload_date", "url", "timestamp_hms", "start_seconds",
               "kind", "setup_type", "confidence", "match_text", "nearby_tickers", "validated_tickers", "context"])

    five_star_rows = [asdict(h) for h in hits if h.kind == "five_star_mention"]
    write_csv(outdir / "five_star_hits.csv", five_star_rows,
              ["video_id", "title", "upload_date", "url", "timestamp_hms", "start_seconds",
               "kind", "setup_type", "confidence", "match_text", "nearby_tickers", "validated_tickers", "context"])

    ticker_counts: Dict[str, int] = {}
    validated_counts: Dict[str, int] = {}
    for h in hits:
        for t in filter(None, (h.nearby_tickers or "").split(",")):
            ticker_counts[t] = ticker_counts.get(t, 0) + 1
        for t in filter(None, (h.validated_tickers or "").split(",")):
            validated_counts[t] = validated_counts.get(t, 0) + 1

    write_csv(outdir / "ticker_counts.csv",
              [{"ticker": k, "mentions": v} for k, v in sorted(ticker_counts.items(), key=lambda x: (-x[1], x[0]))],
              ["ticker", "mentions"])

    write_csv(outdir / "validated_ticker_counts.csv",
              [{"ticker": k, "mentions": v} for k, v in sorted(validated_counts.items(), key=lambda x: (-x[1], x[0]))],
              ["ticker", "mentions"])

    style = style_summary("\n".join(full_text_chunks))
    (outdir / "style_summary.json").write_text(json.dumps(style, indent=2), encoding="utf-8")

    # Simple markdown report
    ok_count = sum(1 for s in statuses if s.transcript_ok)
    md = []
    md.append("# Qullamaggie Stream Harvester v2 Report")
    md.append("")
    md.append(f"- Videos scanned: {len(videos)}")
    md.append(f"- Transcripts fetched: {ok_count}")
    md.append(f"- Five-star mentions: {len(five_star_rows)}")
    md.append(f"- Total hit rows: {len(hit_rows)}")
    md.append("")
    md.append("## Style summary")
    for k, v in style.items():
        md.append(f"- {k}: {v}")
    md.append("")
    md.append("## Top validated tickers")
    for row in sorted(validated_counts.items(), key=lambda x: (-x[1], x[0]))[:50]:
        md.append(f"- {row[0]}: {row[1]}")
    (outdir / "report.md").write_text("\n".join(md), encoding="utf-8")

    print(f"Finished. Output written to {outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
