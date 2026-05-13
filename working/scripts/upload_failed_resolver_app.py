"""
Streamlit UI for manually resolving upload_failed_*.jsonl rows.

Run:
    streamlit run upload_failed_resolver_app.py
"""

from __future__ import annotations

import hashlib
import html
import json
import logging
import re
import tempfile
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import requests
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv

load_dotenv()

from upload.mediawiki import configure_throttle, get_page_content, save_page
from upload.page_metadata import build_case_title_from_content, parse_header_metadata
from upload.uploader import upload_document


logging.getLogger("pywiki").setLevel(logging.WARNING)
logging.getLogger("pywikibot").setLevel(logging.WARNING)

API_URL = "https://zh.wikisource.org/w/api.php"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOG_DIR = PROJECT_ROOT / "working" / "output" / "manual_resolver_logs"
EXISTING_HEADER_METADATA_ERROR = "Could not extract court/type/year from existing page header"
MISSING_HEADER_FIELDS = ("court", "type", "案号")
ID_FIELD_PATTERN = re.compile(r'"(?:wenshu_id|wenshuID|wsKey)"\s*:\s*"([^"]+)"')


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def wiki_url(title: Optional[str]) -> str:
    if not title:
        return ""
    normalized = title.replace(" ", "_")
    return f"https://zh.wikisource.org/wiki/{quote(normalized, safe='/:')}"


def wiki_link(title: Optional[str], label: Optional[str] = None) -> str:
    if not title:
        return ""
    return f"[{label or title}]({wiki_url(title)})"


def linkify_wikitext_titles(text: str) -> str:
    escaped = html.escape(text or "")

    def repl(match: re.Match[str]) -> str:
        title = match.group(1).strip()
        return wiki_link(title, f"[[{html.escape(title)}]]")

    return re.sub(r"\[\[([^\]|#]+)(?:#[^\]|]*)?(?:\|[^\]]*)?\]\]", repl, escaped)


def record_id(record: dict[str, Any], index: int) -> str:
    raw = "|".join(
        str(record.get(key) or "")
        for key in ("source_line", "line_num", "wenshu_id", "title", "timestamp")
    )
    if not raw.strip("|"):
        raw = f"row-{index}"
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()[:12]


def row_wenshu_id(row: dict[str, Any]) -> str:
    return str(row.get("wenshu_id") or row.get("wenshuID") or row.get("wsKey") or "")


def normalize_local_path(path_text: str) -> Path:
    return Path(path_text.strip().strip('"')).expanduser()


def read_jsonl_path(path_text: str) -> list[dict[str, Any]]:
    path = normalize_local_path(path_text)
    rows: list[dict[str, Any]] = []

    with path.open("r", encoding="utf-8-sig") as handle:
        for line_num, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                item = json.loads(stripped)
            except json.JSONDecodeError as exc:
                rows.append(
                    {
                        "source_line": line_num,
                        "status": "failed",
                        "message": f"JSON parse error in {path}: {exc}",
                        "wenshu_id": "",
                        "title": "",
                        "wikitext": "",
                    }
                )
                continue
            if isinstance(item, dict):
                item.setdefault("source_line", line_num)
                rows.append(item)
    return rows


def build_source_index_from_path(path_text: str, wanted_ids: set[str]) -> tuple[dict[str, dict[str, Any]], int]:
    path = normalize_local_path(path_text)
    by_id: dict[str, dict[str, Any]] = {}
    scanned = 0

    with path.open("r", encoding="utf-8-sig") as handle:
        for line in handle:
            scanned += 1
            match = ID_FIELD_PATTERN.search(line)
            if not match or match.group(1) not in wanted_ids:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            wenshu_id = row_wenshu_id(row)
            if wenshu_id in wanted_ids:
                by_id[wenshu_id] = row
                if len(by_id) >= len(wanted_ids):
                    break

    return by_id, scanned


def failure_message(record: dict[str, Any]) -> str:
    return str(record.get("message") or record.get("error") or "")


def classify_failure(record: dict[str, Any]) -> str:
    message = failure_message(record)
    lower = message.lower()
    if EXISTING_HEADER_METADATA_ERROR.lower() in lower:
        return "existing_header_metadata"
    if "court mismatch" in lower:
        return "court_mismatch"
    if "requested page title is too long" in lower or "title is too long" in lower:
        return "title"
    if "illegal" in lower or "invalid title" in lower or "bad title" in lower:
        return "title"
    return "generic"


def find_missing_header_fields(wikitext: str) -> list[str]:
    metadata = parse_header_metadata(wikitext) or {}
    return [field for field in MISSING_HEADER_FIELDS if not metadata.get(field, "").strip()]


@st.cache_data(ttl=15, show_spinner=False)
def render_wikitext_preview(title: str, wikitext: str, nonce: int) -> tuple[str, Optional[str]]:
    if not wikitext.strip():
        return "<p><em>No wikitext to preview.</em></p>", None

    try:
        response = requests.post(
            API_URL,
            data={
                "action": "parse",
                "format": "json",
                "formatversion": "2",
                "contentmodel": "wikitext",
                "prop": "text",
                "disablelimitreport": "1",
                "title": title or "Wikitext preview",
                "text": wikitext,
            },
            headers={
                "User-Agent": "prc-court-docs-upload-failure-resolver/1.0",
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return "", f"Preview request failed: {exc}"

    if "error" in payload:
        error = payload["error"]
        return "", f"{error.get('code', 'error')}: {error.get('info', error)}"

    return payload.get("parse", {}).get("text", "") or "", None


def get_component_func():
    component_dir = Path(tempfile.gettempdir()) / "prc_upload_resolver_codemirror_component"
    component_dir.mkdir(parents=True, exist_ok=True)
    index_path = component_dir / "index.html"
    index_path.write_text(
        r"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <style>
    html, body { margin: 0; padding: 0; background: #fff; font-family: sans-serif; }
    #editor { display: none; }
    .cm-editor {
      border: 1px solid #d0d7de;
      border-radius: 6px;
      font-family: Consolas, "SFMono-Regular", Menlo, monospace;
      font-size: 13px;
      line-height: 1.45;
      outline: none;
    }
    .cm-editor.cm-focused {
      border-color: #7aa7e9;
      box-shadow: 0 0 0 1px #7aa7e9;
    }
    .cm-scroller,
    .cm-content,
    .cm-gutters {
      font-family: Consolas, "SFMono-Regular", Menlo, monospace;
      font-size: 13px;
      line-height: 1.45;
    }
    .cm-scroller {
      overflow: auto;
    }
    .cm-gutters {
      background: #f6f8fa;
      border-right: 1px solid #d0d7de;
      color: #6e7781;
    }
    .cm-activeLine,
    .cm-activeLineGutter {
      background: #f6f8fa;
    }
    textarea.fallback {
      width: calc(100% - 12px);
      border: 1px solid #d0d7de;
      border-radius: 6px;
      padding: 8px;
      font-family: Consolas, "SFMono-Regular", Menlo, monospace;
      font-size: 13px;
      line-height: 1.45;
      resize: vertical;
    }
  </style>
</head>
<body>
  <textarea id="editor"></textarea>
  <script type="module">
    const CODEMIRROR_MEDIAWIKI_URL = "https://cdn.jsdelivr.net/npm/@bhsd/codemirror-mediawiki@3.17.1/dist/main.min.js";
    const CODEMIRROR_UTIL_URL = "https://cdn.jsdelivr.net/npm/@bhsd/cm-util@1.1.0/+esm";
    const SITEINFO_URL = "https://zh.wikisource.org/w/api.php?action=query&meta=siteinfo&siprop=general|magicwords|extensiontags|functionhooks|variables|namespaces|namespacealiases&formatversion=2&format=json&origin=*";
    const ARTICLE_PATH = "https://zh.wikisource.org/wiki/";
    const MEDIAWIKI_URL_PROTOCOLS = "bitcoin:|ftp://|ftps://|geo:|git://|gopher://|http://|https://|irc://|ircs://|magnet:|mailto:|matrix:|mms://|news:|nntp://|redis://|sftp://|sip:|sips:|sms:|ssh://|svn://|tel:|telnet://|urn:|wikipedia://|worldwind://|xmpp:|//";
    const TAG_MODES = {
      onlyinclude: "mediawiki",
      includeonly: "mediawiki",
      noinclude: "mediawiki",
      translate: "mediawiki",
      tvar: "mediawiki",
      pre: "text/pre",
      nowiki: "text/nowiki",
      indicator: "mediawiki",
      poem: "mediawiki",
      ref: "mediawiki",
      references: "text/references",
      gallery: "text/gallery",
      choose: "text/choose",
      option: "mediawiki",
      combobox: "text/combobox",
      combooption: "mediawiki",
      inputbox: "text/inputbox",
      templatedata: "json",
      maplink: "jsonc",
      mapframe: "jsonc",
      math: "text/math",
      chem: "text/math",
      ce: "text/math",
      score: "lilypond"
    };

    let editor = null;
    let fallback = null;
    let timer = null;
    let debounceTimer = null;
    let lastSentValue = null;
    let pendingSentValue = null;
    let suppressInputValue = null;
    let editorInitPromise = null;
    let mediaWikiModulePromise = null;
    let mediaWikiConfigPromise = null;
    let mediaWikiUtilPromise = null;

    function send(type, data) {
      window.parent.postMessage(Object.assign({isStreamlitMessage: true, type: type}, data || {}), "*");
    }

    function setHeight(height) {
      send("streamlit:setFrameHeight", {height: height});
    }

    function setValue(value) {
      lastSentValue = value;
      pendingSentValue = value;
      send("streamlit:setComponentValue", {value: value});
    }

    function getCurrentValue() {
      if (editor && editor.view) {
        return editor.view.state.doc.toString();
      }
      if (fallback) {
        return fallback.value;
      }
      return "";
    }

    async function loadMediaWikiModule() {
      if (!mediaWikiModulePromise) {
        mediaWikiModulePromise = import(CODEMIRROR_MEDIAWIKI_URL).then(function(mod) {
          mod.registerMediaWiki(ARTICLE_PATH);
          if (mod.registerBracketMatchingForMediaWiki) {
            mod.registerBracketMatchingForMediaWiki();
          }
          if (mod.registerCodeFoldingForMediaWiki) {
            mod.registerCodeFoldingForMediaWiki();
          }
          if (mod.registerCloseTagsForMediaWiki) {
            mod.registerCloseTagsForMediaWiki();
          }
          return mod;
        });
      }
      return mediaWikiModulePromise;
    }

    async function loadMediaWikiUtil() {
      if (!mediaWikiUtilPromise) {
        mediaWikiUtilPromise = import(CODEMIRROR_UTIL_URL);
      }
      return mediaWikiUtilPromise;
    }

    function normalizeNamespaceName(name) {
      return String(name || "").replace(/_/g, " ").trim().toLowerCase();
    }

    function buildNamespaceIds(namespaces, namespaceAliases) {
      const nsid = {};
      Object.values(namespaces || {}).forEach(function(ns) {
        const id = Number(ns.id);
        [ns.name, ns.canonical].forEach(function(name) {
          const normalized = normalizeNamespaceName(name);
          if (normalized || id === 0) {
            nsid[normalized] = id;
          }
        });
      });
      (namespaceAliases || []).forEach(function(alias) {
        nsid[normalizeNamespaceName(alias.alias)] = Number(alias.id);
      });
      return nsid;
    }

    function extensionTagName(tag) {
      return String(tag || "").replace(/^<\/?|>$/g, "").toLowerCase();
    }

    function getConfigPair(util, magicwords, rule) {
      return [true, false].map(function(caseSensitive) {
        return util.getConfig(magicwords, rule, caseSensitive);
      });
    }

    async function loadMediaWikiConfig() {
      if (!mediaWikiConfigPromise) {
        mediaWikiConfigPromise = Promise.all([
          loadMediaWikiUtil(),
          fetch(SITEINFO_URL).then(function(response) {
            if (!response.ok) {
              throw new Error("Failed to load zh.wikisource siteinfo: HTTP " + response.status);
            }
            return response.json();
          })
        ]).then(function(values) {
          const util = values[0];
          const query = values[1].query || {};
          const magicwords = query.magicwords || [];
          const variables = query.variables || [];
          const functionHooks = (query.functionhooks || []).map(function(name) {
            return String(name).toLowerCase();
          });
          const otherFunctions = util.otherParserFunctions || new Set(["msgnw"]);
          const functionNames = new Set([
            ...functionHooks,
            ...variables.map(function(name) {
              return String(name).toLowerCase();
            }),
            ...Array.from(otherFunctions)
          ]);
          const functionSynonyms = getConfigPair(util, magicwords, function(word) {
            return functionNames.has(String(word.name).toLowerCase());
          });
          util.cleanAliases(functionSynonyms[1]);

          const extensionTags = (query.extensiontags || [])
            .map(extensionTagName)
            .filter(Boolean);
          const keywords = util.getKeywords(magicwords, true);
          const normalizedFunctionHooks = functionHooks.includes("msgnw")
            ? functionHooks
            : [...functionHooks, "msgnw"];

          return {
            tags: Object.fromEntries(extensionTags.map(function(tag) {
              return [tag, true];
            })),
            tagModes: TAG_MODES,
            doubleUnderscore: getConfigPair(util, magicwords, function(word) {
              return word.aliases.some(function(alias) {
                return /^__.+__$/u.test(alias);
              });
            }),
            functionHooks: normalizedFunctionHooks,
            variableIDs: variables,
            functionSynonyms: functionSynonyms,
            urlProtocols: MEDIAWIKI_URL_PROTOCOLS,
            nsid: buildNamespaceIds(query.namespaces, query.namespacealiases),
            img: keywords.img,
            redirection: keywords.redirection,
            variants: query.general && query.general.langconversion
              ? util.getVariants(query.general.variants)
              : [],
            articlePath: ARTICLE_PATH
          };
        });
      }
      return mediaWikiConfigPromise;
    }

    function initFallback(args, reason) {
      const textarea = document.getElementById("editor");
      const height = Number(args.height || 640);
      const value = args.value || "";
      const debounceMs = Number(args.debounce_ms || 1200);

      if (reason) {
        console.error(reason);
      }
      fallback = textarea;
      fallback.className = "fallback";
      fallback.style.display = "block";
      fallback.style.height = height + "px";
      fallback.value = value;
      fallback.addEventListener("input", function() {
        if (suppressInputValue !== null && fallback.value === suppressInputValue) {
          return;
        }
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(function() {
          setValue(fallback.value);
        }, debounceMs);
      });

      setHeight(height + 8);
    }

    async function initEditor(args) {
      const textarea = document.getElementById("editor");
      const height = Number(args.height || 640);
      const value = args.value || "";
      const debounceMs = Number(args.debounce_ms || 1200);

      textarea.value = value;

      try {
        const mod = await loadMediaWikiModule();
        const mwConfig = await loadMediaWikiConfig();
        editor = new mod.CodeMirror6(textarea, "mediawiki", mwConfig);
        editor.setIndent(2);
        editor.setLineWrapping(true);
        editor.setContent(value, true);
        if (editor.view) {
          editor.view.dom.style.height = height + "px";
        }

        textarea.addEventListener("input", function() {
          const current = getCurrentValue();
          if (suppressInputValue !== null && current === suppressInputValue) {
            return;
          }
          clearTimeout(debounceTimer);
          debounceTimer = setTimeout(function() {
            setValue(current);
          }, debounceMs);
        });
        setHeight(height + 8);
      } catch (error) {
        initFallback(args, error && (error.stack || error.message || String(error)));
      }
    }

    function clearSuppressedInput(value) {
      window.setTimeout(function() {
        if (suppressInputValue === value) {
          suppressInputValue = null;
        }
      }, 900);
    }

    function applyExternalValue(nextValue) {
      suppressInputValue = nextValue;
      if (editor && editor.view && getCurrentValue() !== nextValue) {
        editor.setContent(nextValue, true);
        clearSuppressedInput(nextValue);
      } else if (fallback && fallback.value !== nextValue) {
        fallback.value = nextValue;
        clearSuppressedInput(nextValue);
      }
    }

    function updateEditor(args) {
      const nextValue = args.value || "";
      if (pendingSentValue !== null) {
        if (nextValue === pendingSentValue) {
          pendingSentValue = null;
        } else {
          return;
        }
      }

      if (getCurrentValue() !== nextValue && lastSentValue !== nextValue) {
        applyExternalValue(nextValue);
      }
    }

    function initTimer(args) {
      const interval = Math.max(Number(args.interval_ms || 15000), 1000);
      setHeight(0);
      clearTimeout(timer);
      timer = setTimeout(function() {
        setValue(Date.now());
      }, interval);
    }

    window.addEventListener("message", function(event) {
      const data = event.data || {};
      if (data.type !== "streamlit:render") {
        return;
      }
      const args = data.args || {};
      if (args.kind === "timer") {
        if (!timer) {
          initTimer(args);
        }
        return;
      }
      if (!editor && !fallback) {
        if (!editorInitPromise) {
          editorInitPromise = initEditor(args);
        } else {
          editorInitPromise.then(function() {
            updateEditor(args);
          });
        }
      } else {
        updateEditor(args);
      }
    });

    send("streamlit:componentReady", {apiVersion: 1});
  </script>
</body>
</html>
""",
        encoding="utf-8",
    )
    return components.declare_component("prc_upload_resolver_codemirror", path=str(component_dir))


_component_func = get_component_func()


def codemirror_editor(value: str, *, key: str, height: int = 640) -> str:
    returned = _component_func(
        kind="editor",
        value=value,
        mode="mediawiki",
        theme="eclipse",
        height=height,
        debounce_ms=1200,
        key=key,
        default=value,
    )
    return returned if isinstance(returned, str) else value


def preview_delayed_rerun(wait_seconds: float, *, key: str) -> None:
    interval_ms = max(int(wait_seconds * 1000), 1000)
    _component_func(kind="timer", interval_ms=interval_ms, key=key, default=0)


def ensure_log_path() -> Path:
    if "manual_log_path" not in st.session_state:
        DEFAULT_LOG_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.session_state.manual_log_path = str(DEFAULT_LOG_DIR / f"manual_resolver_{timestamp}.jsonl")
    return Path(st.session_state.manual_log_path)


def append_log(event: str, record: dict[str, Any], **extra: Any) -> None:
    log_path = ensure_log_path()
    entry = {
        "timestamp": utc_now_iso(),
        "event": event,
        "source_line": record.get("source_line") or record.get("line_num"),
        "wenshu_id": record.get("wenshu_id") or record.get("wenshuID") or "",
        "title": record.get("title") or "",
        "case_title": record.get("case_title") or "",
        **extra,
    }
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def read_log_tail(limit: int = 80) -> list[dict[str, Any]]:
    log_path = ensure_log_path()
    if not log_path.exists():
        return []
    lines = log_path.read_text(encoding="utf-8").splitlines()[-limit:]
    entries: list[dict[str, Any]] = []
    for line in lines:
        try:
            loaded = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(loaded, dict):
            entries.append(loaded)
    return entries


def render_log_panel() -> None:
    log_path = ensure_log_path()
    st.caption(f"Persistent log: `{log_path}`")
    entries = read_log_tail()
    if not entries:
        st.info("No actions logged yet.")
        return

    for entry in reversed(entries):
        event = html.escape(str(entry.get("event") or "event"))
        timestamp = html.escape(str(entry.get("timestamp") or ""))
        message = entry.get("message") or entry.get("result_message") or entry.get("error") or ""
        links = []
        for label, key in (("title", "title"), ("final", "final_title"), ("case", "case_title")):
            title = entry.get(key)
            if title:
                links.append(wiki_link(str(title), label))
        rendered_message = linkify_wikitext_titles(str(message))
        st.markdown(
            f"**{timestamp}**  \n"
            f"`{event}`"
            + (f" · {' · '.join(links)}" if links else "")
            + (f"  \n{rendered_message}" if rendered_message else "")
        )
        st.divider()


def get_draft_from_source(record: dict[str, Any]) -> Optional[dict[str, Any]]:
    source_index = st.session_state.get("source_index") or {}
    wenshu_id = row_wenshu_id(record)
    if not wenshu_id:
        return None
    return source_index.get(str(wenshu_id))


def initialize_editor_state(record: dict[str, Any], index: int) -> tuple[str, str, str, str]:
    rid = record_id(record, index)
    prefix = f"row_{rid}"
    source_row = get_draft_from_source(record) or {}

    default_title = (
        record.get("final_title")
        or record.get("title")
        or source_row.get("title")
        or ""
    )
    draft_wikitext = record.get("wikitext") or source_row.get("wikitext") or ""
    default_case_title = (
        record.get("case_title")
        or build_case_title_from_content(draft_wikitext)
        or source_row.get("case_title")
        or ""
    )

    if f"{prefix}_title" not in st.session_state:
        st.session_state[f"{prefix}_title"] = default_title
    if f"{prefix}_case_title" not in st.session_state:
        st.session_state[f"{prefix}_case_title"] = default_case_title
    if f"{prefix}_wikitext" not in st.session_state:
        st.session_state[f"{prefix}_wikitext"] = draft_wikitext
    if f"{prefix}_editor_target" not in st.session_state:
        st.session_state[f"{prefix}_editor_target"] = (
            "existing" if classify_failure(record) == "existing_header_metadata" else "draft"
        )

    return prefix, default_title, default_case_title, draft_wikitext


def set_current_editor_target(prefix: str, target: str, wikitext: str, case_title: str = "") -> None:
    st.session_state[f"{prefix}_editor_target"] = target
    st.session_state[f"{prefix}_wikitext"] = wikitext or ""
    st.session_state[f"{prefix}_case_title"] = case_title or build_case_title_from_content(wikitext or "") or ""


def get_draft_title(record: dict[str, Any], prefix: str) -> str:
    return (
        st.session_state.get(f"{prefix}_draft_title")
        or record.get("final_title")
        or record.get("title")
        or (get_draft_from_source(record) or {}).get("title")
        or ""
    )


def get_draft_wikitext(record: dict[str, Any]) -> str:
    source_row = get_draft_from_source(record) or {}
    return record.get("wikitext") or source_row.get("wikitext") or ""


def switch_to_draft_editor(record: dict[str, Any], prefix: str, draft_wikitext: Optional[str] = None) -> None:
    source_row = get_draft_from_source(record) or {}
    wikitext = draft_wikitext if draft_wikitext is not None else get_draft_wikitext(record)
    st.session_state[f"{prefix}_draft_title"] = (
        record.get("final_title") or record.get("title") or source_row.get("title") or ""
    )
    st.session_state[f"{prefix}_title"] = st.session_state[f"{prefix}_draft_title"]
    set_current_editor_target(prefix, "draft", wikitext or "", build_case_title_from_content(wikitext or ""))


def fetch_existing_page_into_editor(record: dict[str, Any], prefix: str) -> None:
    title = record.get("title") or st.session_state.get(f"{prefix}_title") or ""
    if not title:
        st.error("No existing title is available to fetch.")
        return
    try:
        exists, content = get_page_content(str(title))
    except Exception as exc:
        append_log("fetch_existing_failed", record, title=title, error=str(exc))
        st.error(f"Could not fetch existing page: {exc}")
        return
    if not exists or content is None:
        st.warning(f"Existing page not found: {title}")
        append_log("fetch_existing_missing", record, title=title)
        return
    set_current_editor_target(prefix, "existing", content, build_case_title_from_content(content))
    append_log("fetch_existing", record, title=title)
    st.rerun()


def auto_fetch_existing_page_into_editor(record: dict[str, Any], prefix: str) -> None:
    marker = f"{prefix}_auto_existing_attempted"
    if st.session_state.get(marker):
        return
    st.session_state[marker] = True

    title = record.get("title") or st.session_state.get(f"{prefix}_title") or ""
    if not title:
        return

    try:
        exists, content = get_page_content(str(title))
    except Exception as exc:
        append_log("auto_fetch_existing_failed", record, title=title, error=str(exc))
        return

    if not exists or content is None:
        append_log("auto_fetch_existing_missing", record, title=title)
        return

    set_current_editor_target(prefix, "existing", content, build_case_title_from_content(content))
    append_log("auto_fetch_existing", record, title=title)


def remove_current_record() -> None:
    records = st.session_state.records
    index = st.session_state.current_index
    if not records:
        return
    records.pop(index)
    if index >= len(records):
        st.session_state.current_index = max(len(records) - 1, 0)
    st.session_state.last_result = None


def move_current_record_to_end(updated_record: dict[str, Any]) -> None:
    records = st.session_state.records
    index = st.session_state.current_index
    records.pop(index)
    updated_record["manual_attempts"] = int(updated_record.get("manual_attempts") or 0) + 1
    records.append(updated_record)
    if index >= len(records):
        st.session_state.current_index = 0
    st.session_state.last_result = None


def apply_editor_values_to_record(record: dict[str, Any], prefix: str, *, target: str) -> dict[str, Any]:
    updated = dict(record)
    if target == "draft":
        updated["title"] = st.session_state.get(f"{prefix}_title", "") or ""
        updated["case_title"] = st.session_state.get(f"{prefix}_case_title", "") or ""
        updated["wikitext"] = st.session_state.get(f"{prefix}_wikitext", "") or ""
    return updated


def submit_draft_upload(
    record: dict[str, Any],
    prefix: str,
    *,
    title: str,
    wikitext: str,
    force_overwrite: bool = False,
) -> None:
    updated = dict(record)
    updated["title"] = title
    updated["case_title"] = build_case_title_from_content(wikitext) or updated.get("case_title") or ""
    updated["wikitext"] = wikitext
    wenshu_id = updated.get("wenshu_id") or updated.get("wenshuID") or ""

    if not title or not wikitext:
        switch_to_draft_editor(record, prefix, wikitext)
        st.error("Both title and wikitext are required before retrying.")
        return

    try:
        configure_throttle(
            interval=float(st.session_state.get("edit_interval", 3.0)),
            maxlag=int(st.session_state.get("maxlag", 5)),
        )
        result = upload_document(
            title=title,
            wenshu_id=str(wenshu_id),
            wikitext=wikitext,
            resolve_conflicts=True,
            force_overwrite=force_overwrite,
        )
    except Exception as exc:
        append_log("retry_exception", updated, title=title, error=str(exc))
        st.session_state.last_result = {"status": "failed", "message": str(exc)}
        switch_to_draft_editor(record, prefix, wikitext)
        st.error(f"Retry failed before uploader returned a result: {exc}")
        return

    result_dict = asdict(result)
    st.session_state.last_result = result_dict
    append_log(
        "retry_upload",
        updated,
        title=result.title,
        final_title=result.final_title,
        case_title=result.case_title,
        status=result.status,
        result_message=result.message,
        force_overwrite=force_overwrite,
    )

    if result.status in {"uploaded", "conflict_resolved", "skipped"}:
        remove_current_record()
        st.success(f"{result.status}: {result.message}")
        st.rerun()

    records = st.session_state.records
    if records:
        records[st.session_state.current_index] = {
            **updated,
            "status": result.status,
            "message": result.message,
            "final_title": result.final_title,
            "case_title": result.case_title or updated.get("case_title"),
            "redirect_status": result.redirect_status,
        }
    switch_to_draft_editor(records[st.session_state.current_index] if records else updated, prefix, wikitext)
    st.rerun()


def retry_upload(record: dict[str, Any], prefix: str, *, force_overwrite: bool = False) -> None:
    submit_draft_upload(
        record,
        prefix,
        title=st.session_state.get(f"{prefix}_title", "") or "",
        wikitext=st.session_state.get(f"{prefix}_wikitext", "") or "",
        force_overwrite=force_overwrite,
    )


def save_existing_page_and_retry(record: dict[str, Any], prefix: str) -> None:
    existing_title = record.get("title") or ""
    edited_existing_wikitext = st.session_state.get(f"{prefix}_wikitext", "") or ""
    if not existing_title or not edited_existing_wikitext:
        st.error("Existing page title and edited wikitext are required.")
        return

    try:
        save_page(
            str(existing_title),
            edited_existing_wikitext,
            "补充裁判文书Header元数据以便消歧义",
            minor=True,
            bot=True,
        )
    except Exception as exc:
        append_log("save_existing_failed", record, title=existing_title, error=str(exc))
        st.session_state.last_result = {"status": "failed", "message": str(exc)}
        st.error(f"Could not save existing page repair: {exc}")
        return

    append_log("save_existing", record, title=existing_title, final_title=existing_title)

    draft_wikitext = get_draft_wikitext(record)
    draft_title = get_draft_title(record, prefix)
    if not draft_wikitext:
        st.session_state.last_result = {
            "status": "saved_existing_only",
            "message": "Existing page saved, but no draft wikitext was available for retry.",
        }
        switch_to_draft_editor(record, prefix, "")
        st.warning("Existing page saved. Load the original converted JSONL or paste draft wikitext to retry.")
        st.rerun()
        return

    submit_draft_upload(
        record,
        prefix,
        title=draft_title,
        wikitext=draft_wikitext,
        force_overwrite=False,
    )


def preview_signature(title: str, wikitext: str) -> str:
    raw = f"{title}\0{wikitext}".encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()


def render_preview_html(rendered: str) -> None:
    rendered = (
        rendered.replace('href="/wiki/', 'href="https://zh.wikisource.org/wiki/')
        .replace('href="/w/', 'href="https://zh.wikisource.org/w/')
        .replace('src="/wiki/', 'src="https://zh.wikisource.org/wiki/')
        .replace('src="/w/', 'src="https://zh.wikisource.org/w/')
    )
    preview_html = f"""
<style>
  .resolver-preview {{
    max-height: 740px;
    overflow: auto;
    padding: 12px 14px 28px;
    border: 1px solid #d0d7de;
    border-radius: 6px;
    background: #fff;
    color: #202122;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    font-size: 14px;
    line-height: 1.55;
  }}
  .resolver-preview a {{ color: #0645ad; text-decoration: none; }}
  .resolver-preview a:hover {{ text-decoration: underline; }}
  .resolver-preview table {{ border-collapse: collapse; max-width: 100%; }}
  .resolver-preview td, .resolver-preview th {{ border: 1px solid #a2a9b1; padding: 4px 6px; }}
  .resolver-preview .mw-parser-output {{ max-width: 100%; overflow-wrap: anywhere; }}
</style>
<div class="resolver-preview">
  <div class="mw-parser-output">{rendered}</div>
</div>"""
    st.html(preview_html)


def render_preview_panel(title: str, wikitext: str) -> None:
    st.subheader("Preview")
    col_a, col_b = st.columns([1, 1])
    with col_a:
        auto_preview = st.toggle(
            "Auto",
            value=True,
            help="Render changed wikitext at most once every 15 seconds.",
        )
    with col_b:
        force_refresh = st.button("Refresh now", use_container_width=True)

    throttle_seconds = 15.0
    signature = preview_signature(title, wikitext)
    now = time.monotonic()
    last_signature = st.session_state.get("preview_last_signature")
    last_rendered_at = float(st.session_state.get("preview_last_rendered_at") or 0.0)
    last_rendered = st.session_state.get("preview_last_rendered") or ""
    last_error = st.session_state.get("preview_last_error")

    changed = signature != last_signature
    should_render = force_refresh or (auto_preview and changed)
    if changed and not auto_preview and not force_refresh:
        st.caption("Preview has unrendered changes; use Refresh now to update it.")
        if last_error:
            st.error(last_error)
            st.code(wikitext[:8000], language="mediawiki")
        elif last_rendered:
            render_preview_html(last_rendered)
        else:
            st.info("Preview is paused.")
        return

    if should_render and not force_refresh and last_rendered_at and now - last_rendered_at < throttle_seconds:
        wait_seconds = throttle_seconds - (now - last_rendered_at)
        st.caption(f"Preview pending; updating in about {max(int(wait_seconds), 1)} seconds.")
        if last_error:
            st.error(last_error)
            st.code(wikitext[:8000], language="mediawiki")
        elif last_rendered:
            render_preview_html(last_rendered)
        else:
            st.info("Preview will render shortly.")
        preview_delayed_rerun(wait_seconds, key=f"preview_wait_{signature[:12]}")
        return

    if should_render:
        nonce = int(st.session_state.get("preview_nonce", 0)) + (1 if force_refresh else 0)
        if force_refresh:
            st.session_state.preview_nonce = nonce
        with st.spinner("Rendering preview..."):
            rendered, error = render_wikitext_preview(title, wikitext, nonce)
        st.session_state.preview_last_signature = signature
        st.session_state.preview_last_rendered_at = now
        st.session_state.preview_last_rendered = rendered
        st.session_state.preview_last_error = error
        last_rendered = rendered
        last_error = error

    if last_error:
        st.error(last_error)
        st.code(wikitext[:8000], language="mediawiki")
        return

    render_preview_html(last_rendered)


def load_records_from_inputs() -> None:
    st.sidebar.subheader("Input")
    failed_path = st.sidebar.text_input("upload_failed JSONL path")
    source_path = st.sidebar.text_input("Original converted JSONL path")

    load_inputs = st.sidebar.button("Load files", type="primary", use_container_width=True)
    if load_inputs:
        if not failed_path.strip() or not source_path.strip():
            st.sidebar.error("Enter both JSONL paths.")
            return

        try:
            rows = read_jsonl_path(failed_path.strip())
        except Exception as exc:
            st.sidebar.error(f"Could not load failed JSONL: {exc}")
            return

        wanted_ids = {row_wenshu_id(row) for row in rows if row_wenshu_id(row)}
        if not wanted_ids:
            st.sidebar.error("The failed JSONL did not contain any wenshu_id/wsKey values.")
            return

        try:
            source_index, scanned = build_source_index_from_path(source_path.strip(), wanted_ids)
        except Exception as exc:
            st.sidebar.error(f"Could not load source JSONL: {exc}")
            return

        for key in list(st.session_state.keys()):
            if str(key).startswith("row_"):
                del st.session_state[key]

        st.session_state.records = rows
        st.session_state.source_index = source_index
        st.session_state.current_index = 0
        st.session_state.last_result = None
        st.session_state.preview_nonce = 0
        st.session_state.show_queue_download = False
        st.session_state.failed_source_name = failed_path.strip()
        st.session_state.source_name = source_path.strip()
        append_log("load_failed_queue", {"title": failed_path.strip()}, count=len(rows))
        append_log(
            "load_source_jsonl",
            {"title": source_path.strip()},
            count=len(source_index),
            wanted=len(wanted_ids),
            scanned=scanned,
        )
        st.rerun()

    st.sidebar.subheader("Upload settings")
    st.sidebar.number_input("Edit interval", min_value=0.0, value=3.0, step=0.5, key="edit_interval")
    st.sidebar.number_input("Maxlag", min_value=1, value=5, step=1, key="maxlag")


def main() -> None:
    st.set_page_config(page_title="Upload Failure Resolver", layout="wide")
    st.title("Upload Failure Resolver")

    if "records" not in st.session_state:
        st.session_state.records = []
    if "current_index" not in st.session_state:
        st.session_state.current_index = 0
    if "source_index" not in st.session_state:
        st.session_state.source_index = {}
    if "preview_nonce" not in st.session_state:
        st.session_state.preview_nonce = 0

    load_records_from_inputs()

    records: list[dict[str, Any]] = st.session_state.records
    total = len(records)
    if not records:
        col_intro, col_logs = st.columns([2, 1])
        with col_intro:
            st.info("Enter both JSONL paths in the sidebar, then load them together.")
            st.markdown(
                "The failed queue is read line by line, and the source JSONL is scanned only for the failed document IDs."
            )
        with col_logs:
            render_log_panel()
        return

    st.session_state.current_index = min(st.session_state.current_index, max(total - 1, 0))
    index = st.session_state.current_index
    record = records[index]
    prefix, _, _, draft_wikitext = initialize_editor_state(record, index)
    source_row = get_draft_from_source(record)
    failure_kind = classify_failure(record)
    if failure_kind == "existing_header_metadata":
        auto_fetch_existing_page_into_editor(record, prefix)
    editor_target = st.session_state.get(f"{prefix}_editor_target", "draft")

    st.caption(
        f"Queue item {index + 1} of {total}"
        + (f" · source: `{st.session_state.get('failed_source_name')}`" if st.session_state.get("failed_source_name") else "")
        + (f" · draft source: `{st.session_state.get('source_name')}`" if st.session_state.get("source_name") else "")
    )

    edit_col, preview_col, log_col = st.columns([1.15, 1.0, 0.9], gap="medium")

    with edit_col:
        st.subheader("Edit")
        message = failure_message(record)
        if message:
            st.warning(message)

        if failure_kind == "existing_header_metadata":
            if editor_target == "existing":
                st.info(
                    "Edit the existing wiki page header first. Saving it will immediately retry the separate draft "
                    "from the selected source JSONL."
                )
            else:
                st.info("The existing page was saved. The retry still failed, so edit the draft upload content now.")
        elif failure_kind == "court_mismatch":
            st.info(
                "The uploader can now resolve cross-court title collisions by creating a court-grouped "
                "裁判文书消歧义页. Try auto-resolution before editing the draft."
            )
        elif failure_kind == "title":
            st.info("Edit the title and, if needed, the wikitext header before retrying.")

        title = st.text_input("Canonical title", key=f"{prefix}_title")
        case_title = st.text_input("Case-number title", key=f"{prefix}_case_title")

        button_cols = st.columns(3)
        with button_cols[0]:
            if editor_target == "existing":
                if st.button("Reload existing", use_container_width=True):
                    fetch_existing_page_into_editor(record, prefix)
        with button_cols[1]:
            if editor_target == "draft" and source_row and st.button("Use source draft", use_container_width=True):
                switch_to_draft_editor(record, prefix, source_row.get("wikitext") or "")
                st.rerun()
        with button_cols[2]:
            if st.button("Clear result", use_container_width=True):
                st.session_state.last_result = None
                st.rerun()

        wikitext = codemirror_editor(
            st.session_state.get(f"{prefix}_wikitext", "") or "",
            key=f"{prefix}_editor",
            height=650,
        )
        if wikitext != st.session_state.get(f"{prefix}_wikitext"):
            st.session_state[f"{prefix}_wikitext"] = wikitext

        parsed_case_title = build_case_title_from_content(wikitext)
        missing_fields = find_missing_header_fields(wikitext)
        if parsed_case_title:
            st.caption(f"Parsed case-number title: {wiki_link(parsed_case_title)}")
        if missing_fields:
            st.caption(f"Missing header fields: `{', '.join(missing_fields)}`")
        if case_title and parsed_case_title and case_title != parsed_case_title:
            st.caption("Case-number textbox differs from the title parsed from wikitext.")
        if not draft_wikitext and not source_row and editor_target == "draft":
            st.error("This failed row has no draft wikitext. Load the original converted JSONL or paste content here.")
        if failure_kind == "existing_header_metadata" and editor_target == "existing" and not source_row and not record.get("wikitext"):
            st.caption("Load the matching source JSONL before saving if you want the app to retry the draft automatically.")

        action_cols = st.columns(3)
        with action_cols[0]:
            if editor_target == "existing":
                if st.button("Save + retry", type="primary", use_container_width=True):
                    save_existing_page_and_retry(record, prefix)
            else:
                primary_label = "Auto-resolve" if failure_kind == "court_mismatch" else "Retry upload"
                if st.button(primary_label, type="primary", use_container_width=True):
                    retry_upload(record, prefix, force_overwrite=False)
        with action_cols[1]:
            if editor_target == "draft":
                force_overwrite = st.button("Force overwrite", use_container_width=True)
                if force_overwrite:
                    retry_upload(record, prefix, force_overwrite=True)
        with action_cols[2]:
            updated_for_queue = apply_editor_values_to_record(record, prefix, target=editor_target)
            if st.button("Move to end", use_container_width=True):
                append_log("move_to_end", updated_for_queue, message="Moved to queue end")
                move_current_record_to_end(updated_for_queue)
                st.rerun()

        nav_cols = st.columns(4)
        with nav_cols[0]:
            if st.button("Previous", use_container_width=True, disabled=index <= 0):
                st.session_state.current_index = max(index - 1, 0)
                st.session_state.last_result = None
                st.rerun()
        with nav_cols[1]:
            if st.button("Next", use_container_width=True, disabled=index >= total - 1):
                st.session_state.current_index = min(index + 1, total - 1)
                st.session_state.last_result = None
                st.rerun()
        with nav_cols[2]:
            if st.button("Skip", use_container_width=True):
                updated_for_queue = apply_editor_values_to_record(record, prefix, target=editor_target)
                append_log("skip", updated_for_queue, message="Skipped manually")
                remove_current_record()
                st.rerun()
        with nav_cols[3]:
            if st.button("Download queue", use_container_width=True):
                st.session_state.show_queue_download = True

        last_result = st.session_state.get("last_result")
        if last_result:
            status = last_result.get("status")
            result_message = last_result.get("message") or last_result.get("result_message") or ""
            if status == "failed":
                st.error(f"Retry failed: {result_message}")
                fail_cols = st.columns(3)
                with fail_cols[0]:
                    if st.button("Re-edit", use_container_width=True):
                        st.session_state.last_result = None
                        st.rerun()
                with fail_cols[1]:
                    if st.button("Failed: move end", use_container_width=True):
                        updated_for_queue = apply_editor_values_to_record(record, prefix, target=editor_target)
                        append_log("failed_move_to_end", updated_for_queue, message=result_message)
                        move_current_record_to_end(updated_for_queue)
                        st.rerun()
                with fail_cols[2]:
                    if st.button("Failed: skip", use_container_width=True):
                        updated_for_queue = apply_editor_values_to_record(record, prefix, target=editor_target)
                        append_log("failed_skip", updated_for_queue, message=result_message)
                        remove_current_record()
                        st.rerun()
            else:
                st.info(f"{status}: {result_message}")

        if st.session_state.get("show_queue_download"):
            queue_text = "\n".join(json.dumps(row, ensure_ascii=False) for row in records)
            st.download_button(
                "Download remaining queue JSONL",
                data=queue_text,
                file_name="remaining_upload_failed_queue.jsonl",
                mime="application/jsonl",
                use_container_width=True,
            )

    with log_col:
        st.subheader("Logs")
        render_log_panel()

    with preview_col:
        preview_title = title or record.get("title") or ""
        render_preview_panel(preview_title, st.session_state.get(f"{prefix}_wikitext", "") or "")


if __name__ == "__main__":
    main()
