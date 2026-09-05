"""Phase 4: transformation of Landing Zone documents into the transformed store.

`html_cleaner.py` holds the BeautifulSoup content extraction, kept independent
of storage/orchestration so it is testable against plain HTML strings.
`service.py` holds the framework-agnostic transform + canonical-selection state
machine (mirrors `wrc_scraper.storage.ingest_service`). `cli.py` is the thin
entry point.
"""

from __future__ import annotations
