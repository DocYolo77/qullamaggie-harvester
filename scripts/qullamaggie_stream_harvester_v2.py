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
            duration = item.get("duration") or 0
            title_l = title.lower()

            is_live_meta = item.get("live_status") in {"is_live", "was_live", "post_live"}

            likely_stream = any(x in title_l for x in [
                "ep",
                "earnings",
                "market",
                "setup",
                "setups",
                "stocks",
                "follow through",
                "fades",
                "speculation",
                "quantum",
            ])

            is_long_form = bool(duration and duration >= 20 * 60)

            is_live = is_live_meta or likely_stream or is_long_form

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

    if live_only and not out:
        return collect_videos_ytdlp(channel_url, live_only=False, limit=limit)

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
            title_l = title.lower()

            likely_stream = any(x in title_l for x in [
                "ep",
                "earnings",
                "market",
                "setup",
                "setups",
                "stocks",
                "follow through",
                "fades",
                "speculation",
                "quantum",
            ])

            is_live = likely_stream

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
