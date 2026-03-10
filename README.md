# Qullamaggie Harvester v2

Dieses Repo ist dafür gebaut, Qullamaggie-Streams automatisiert zu scannen und aus öffentlich verfügbaren Untertiteln bzw. Transkripten möglichst viel Struktur herauszuziehen.

## Was diese Version besser macht

- Video-Erfassung per `yt-dlp` oder YouTube Data API
- Transkript-Fallback:
  - zuerst `youtube-transcript-api`
  - wenn das scheitert: `yt-dlp` mit normalen und Auto-Untertiteln
- Parser für `vtt`, `srt` und `json3`
- 5-Star-Erkennung
- grobe Setup-Klassifikation:
  - EP
  - Breakout
  - Parabolic Short
  - Other
- optionale Ticker-Validierung über SEC `company_tickers.json`
- GitHub Actions Workflow
- Ergebnisdateien als Artifact

---

## Was du am Ende bekommst

Im Output-Ordner bzw. Artifact:

- `videos.csv`
- `transcript_status.csv`
- `all_hits.csv`
- `five_star_hits.csv`
- `ticker_counts.csv`
- `validated_ticker_counts.csv`
- `style_summary.json`
- `report.md`
- optional Rohtranskripte unter `transcripts/`

---

## Die Wahrheit vorweg

Das hier ist die beste pragmatische öffentliche Version.  
Wenn ein Stream **gar keine öffentlich abrufbaren Untertitel** hat, kann auch dieses Repo sie nicht magisch erzeugen.

Was diese Version aber macht:
Sie maximiert die Ausbeute aus allem, was öffentlich technisch erreichbar ist.

---

# Schritt für Schritt

## 1. Neues GitHub-Repo erstellen

Auf GitHub:
- `New repository`
- Name zum Beispiel: `qullamaggie-harvester`
- Public oder Private, beides geht

## 2. Dateien hochladen

Lade **alle Dateien und Ordner** aus diesem Paket in dein Repo hoch.

Wichtig:
Die Workflow-Datei muss genau hier liegen:

`.github/workflows/harvest.yml`

## 3. Optional: YouTube API Key als Secret setzen

Das brauchst du nur, wenn du später `mode=api` nutzen willst.

In GitHub:
- Repo öffnen
- `Settings`
- `Secrets and variables`
- `Actions`
- `New repository secret`

Name:
`YOUTUBE_API_KEY`

Wert:
dein API-Key

Wenn du keinen Key hast, ist das egal. Standardmäßig läuft das Repo mit `yt-dlp`.

## 4. Workflow starten

Im Repo:
- Tab `Actions`
- Workflow `Harvest Qullamaggie Streams`
- `Run workflow`

## 5. Gute Start-Einstellungen

Für den ersten Run:

- `channel_url`: `https://www.youtube.com/@qullamaggie/videos`
- `mode`: `ytdlp`
- `channel_id`: leer lassen
- `live_only`: `true`
- `limit`: `100`
- `langs`: `en,en-US`
- `save_all_transcripts`: `true`
- `validate_tickers`: `true`

## 6. Warten bis der Run fertig ist

Danach den Run anklicken.

## 7. Artifact herunterladen

Im fertigen Workflow-Run findest du unten das Artifact:

`qullamaggie-harvest-output`

Das herunterladen und entpacken.

---

# Lokal ausführen

Wenn du doch lokal laufen lassen willst:

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate

pip install -r requirements.txt

python scripts/qullamaggie_stream_harvester_v2.py --channel-url "https://www.youtube.com/@qullamaggie/videos" --live-only --save-all-transcripts --validate-tickers --outdir out
```

---

# Repo-Struktur

```text
.
├─ .github/
│  └─ workflows/
│     └─ harvest.yml
├─ scripts/
│  └─ qullamaggie_stream_harvester_v2.py
├─ requirements.txt
├─ .gitignore
└─ README.md
```

---

# Wie ich danach mit den Daten weiterarbeiten würde

Wenn der erste Harvest läuft, ist der nächste sinnvolle Schritt:

1. `five_star_hits.csv` sortieren
2. `validated_ticker_counts.csv` prüfen
3. die Top-Kandidaten manuell oder per zweiter NLP-Stufe validieren
4. daraus ein echtes Qullamaggie-Masterdokument bauen

---

# Schwächen der aktuellen Version

- Ticker-Erkennung bleibt bei gesprochenem Text nie perfekt
- Auto-Untertitel können Ticker falsch schreiben
- Setup-Klassifikation ist regelbasiert, nicht semantisch voll ausgereift

---

# Nächste sinnvolle Ausbaustufe

Wenn du noch mehr willst, wäre Version 3:

- bessere semantische Extraktion über LLM oder spaCy
- dedizierte Erkennung von:
  - 5-Star
  - 5-Star Plus
  - EP
  - Breakout
  - Parabolic Short
- Ticker-Normalisierung mit Alias-Mapping
- DOCX-Endreport
- SQLite-Datenbank
