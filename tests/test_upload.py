"""
Tests for upload-time redirect creation, batching, and conflict resolution.
"""

import json
from textwrap import dedent

from upload import mediawiki
from upload.mediawiki import PageSnapshot, ResolvedPage
from upload import uploader
from upload import conflict_resolution
from upload.page_metadata import extract_redirect_target


def make_header_page(
    title: str,
    court: str,
    doc_type: str,
    case_number: str,
    year: str = "2024",
) -> str:
    return dedent(
        f"""\
        {{{{Header/裁判文书
        |title = {title}
        |court = {court}
        |type = {doc_type}
        |案号 = {case_number}
        |year = {year}
        |month = 1
        |day = 1
        |loc = 
        |docid = doc-1
        }}}}
        正文。
        {{{{PD-PRC-exempt}}}}
        """
    )


def make_versions_page(title: str, court: str, entry_title: str) -> str:
    return dedent(
        f"""\
        {{{{裁判文书消歧义页
         | title      = {title}
         | court      = {court}
         | type       = 民事判决书
         | year       = 2024
        }}}}
        * [[{entry_title}]]
        """
    )


def make_legacy_versions_page(title: str, court: str, entry_title: str) -> str:
    return dedent(
        f"""\
        {{{{versions
         | title      = {title}
         | noauthor   = {court}
         | portal     =
         | notes      =
        }}}}
        * [[{entry_title}]]

        [[Category:2024年民事判决书]]
        [[Category:民事判决书]]
        [[Category:{court}]]
        """
    )


def test_post_query_uses_pywikibot_simple_request(monkeypatch):
    seen = {}

    class FakeRequest:
        def submit(self):
            seen["submitted"] = True
            return {"query": {"pages": []}}

    class FakeSite:
        def simple_request(self, **kwargs):
            seen["params"] = kwargs
            return FakeRequest()

    monkeypatch.setattr(mediawiki, "get_site", lambda: FakeSite())

    payload = mediawiki.post_query(
        {
            "titles": "甲|乙",
            "prop": "revisions",
            "rvprop": "content",
        },
        maxlag=9,
    )

    assert payload == {"query": {"pages": []}}
    assert seen["submitted"] is True
    assert seen["params"] == {
        "action": "query",
        "format": "json",
        "formatversion": "2",
        "maxlag": 9,
        "titles": "甲|乙",
        "prop": "revisions",
        "rvprop": "content",
    }


def test_upload_document_creates_case_redirect_after_upload(monkeypatch):
    title = "张三与李四民事判决书"
    wikitext = make_header_page(
        title=title,
        court="北京市第一中级人民法院",
        doc_type="民事判决书",
        case_number="（2024）京01民终1号",
    )
    case_title = "北京市第一中级人民法院（2024）京01民终1号民事判决书"
    saves = []
    resolve_calls = {"count": 0}

    def fake_resolve_page(requested_title):
        assert requested_title == case_title
        resolve_calls["count"] += 1
        return ResolvedPage(requested_title=requested_title, exists=False)

    def fake_save_page(target_title, content, summary, **kwargs):
        saves.append((target_title, content, summary))
        return True

    monkeypatch.setattr(uploader, "resolve_page", fake_resolve_page)
    monkeypatch.setattr(uploader, "check_page_exists", lambda requested_title: (False, None))
    monkeypatch.setattr(uploader, "save_page", fake_save_page)

    result = uploader.upload_document(title=title, wenshu_id="doc-1", wikitext=wikitext)

    assert result.status == "uploaded"
    assert result.final_title == title
    assert result.case_title == case_title
    assert result.redirect_status == "created"
    assert resolve_calls["count"] == 1
    assert saves[0][0] == title
    assert saves[1][0] == case_title
    assert "#REDIRECT [[张三与李四民事判决书]]" in saves[1][1]


def test_upload_document_skips_when_case_page_already_lands_on_header(monkeypatch):
    title = "张三与李四民事判决书"
    landing_title = "北京市第一中级人民法院（2024）京01民终1号民事判决书"
    wikitext = make_header_page(
        title=title,
        court="北京市第一中级人民法院",
        doc_type="民事判决书",
        case_number="（2024）京01民终1号",
    )

    monkeypatch.setattr(
        uploader,
        "resolve_page",
        lambda requested_title: ResolvedPage(
            requested_title=requested_title,
            exists=True,
            is_redirect=True,
            redirect_target=landing_title,
            resolved_title=landing_title,
            content=make_header_page(
                title=title,
                court="北京市第一中级人民法院",
                doc_type="民事判决书",
                case_number="（2024）京01民终1号",
            ),
        ),
    )
    monkeypatch.setattr(uploader, "check_page_exists", lambda requested_title: (False, None))
    monkeypatch.setattr(
        uploader,
        "save_page",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("save_page should not be called")),
    )

    result = uploader.upload_document(title=title, wenshu_id="doc-1", wikitext=wikitext)

    assert result.status == "skipped"
    assert result.final_title == landing_title
    assert result.case_title == landing_title
    assert result.redirect_status == "existing"
    assert "Case-number page already exists" in result.message


def test_upload_document_skips_identical_content_despite_line_ending_difference(monkeypatch):
    title = "张三与李四民事判决书"
    wikitext = make_header_page(
        title=title,
        court="北京市第一中级人民法院",
        doc_type="民事判决书",
        case_number="（2024）京01民终1号",
    )
    case_title = "北京市第一中级人民法院（2024）京01民终1号民事判决书"
    existing_content = wikitext.replace("\n", "\r\n") + "\r\n"

    monkeypatch.setattr(
        uploader,
        "resolve_page",
        lambda requested_title: ResolvedPage(
            requested_title=requested_title,
            exists=True,
            is_redirect=True,
            redirect_target=title,
            resolved_title=title,
            content=existing_content,
        ) if requested_title == case_title else ResolvedPage(requested_title=requested_title, exists=False),
    )
    monkeypatch.setattr(uploader, "check_page_exists", lambda requested_title: (True, 1))
    monkeypatch.setattr(uploader, "get_page_content", lambda requested_title: (True, existing_content))
    monkeypatch.setattr(
        uploader,
        "save_page",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("save_page should not be called")),
    )

    result = uploader.upload_document(title=title, wenshu_id="doc-1", wikitext=wikitext)

    assert result.status == "skipped"
    assert result.final_title == title
    assert result.case_title == case_title
    assert result.redirect_status == "existing"
    assert result.message == "Content identical to existing page"


def test_upload_document_skips_after_versions_resolution_when_case_page_exists(monkeypatch):
    title = "张三与李四民事判决书"
    court = "北京市第一中级人民法院"
    case_title = "北京市第一中级人民法院（2024）京01民终1号民事判决书"
    wikitext = make_header_page(
        title=title,
        court=court,
        doc_type="民事判决书",
        case_number="（2024）京01民终1号",
    )
    versions_content = make_versions_page(title=title, court=court, entry_title=case_title)
    resolve_calls = {"count": 0}

    def fake_resolve_page(requested_title):
        assert requested_title == case_title
        resolve_calls["count"] += 1
        return ResolvedPage(
            requested_title=requested_title,
            exists=True,
            is_redirect=True,
            redirect_target="已有落点",
            resolved_title="已有落点",
            content=make_header_page(
                title=title,
                court=court,
                doc_type="民事判决书",
                case_number="（2024）京01民终1号",
            ),
        )

    monkeypatch.setattr(uploader, "resolve_page", fake_resolve_page)
    monkeypatch.setattr(uploader, "check_page_exists", lambda requested_title: (True, 1))
    monkeypatch.setattr(uploader, "get_page_content", lambda requested_title: (True, versions_content))
    monkeypatch.setattr(
        uploader,
        "try_resolve_conflict",
        lambda original_title, draft_content, existing_content: (True, case_title, None),
    )
    monkeypatch.setattr(
        uploader,
        "update_draft_for_conflict_resolution",
        lambda draft_content, original_title: draft_content,
    )
    monkeypatch.setattr(
        uploader,
        "save_page",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("save_page should not be called")),
    )

    result = uploader.upload_document(title=title, wenshu_id="doc-1", wikitext=wikitext)

    assert result.status == "skipped"
    assert result.final_title == "已有落点"
    assert result.case_title == case_title
    assert result.redirect_status == "existing"
    assert resolve_calls["count"] == 1


def test_try_resolve_conflict_skips_unchanged_versions_page_save(monkeypatch):
    original_title = "共享标题"
    court = "北京市第一中级人民法院"
    case_title = "北京市第一中级人民法院（2024）京01民终1号民事判决书"
    draft_content = make_header_page(
        title=original_title,
        court=court,
        doc_type="民事判决书",
        case_number="（2024）京01民终1号",
    )
    existing_content = make_versions_page(title=original_title, court=court, entry_title=case_title)

    monkeypatch.setattr(
        conflict_resolution,
        "save_page",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("save_page should not be called")),
    )

    resolved, new_title, error = conflict_resolution.try_resolve_conflict(
        original_title=original_title,
        draft_content=draft_content,
        existing_content=existing_content,
    )

    assert resolved is True
    assert new_title == case_title
    assert error is None


def test_try_resolve_conflict_accepts_legacy_versions_page(monkeypatch):
    original_title = "共享标题"
    court = "北京市第一中级人民法院"
    existing_case_title = "北京市第一中级人民法院（2024）京01民终1号民事判决书"
    draft_case_title = "北京市第一中级人民法院（2024）京01民终2号民事判决书"
    draft_content = make_header_page(
        title=original_title,
        court=court,
        doc_type="民事判决书",
        case_number="（2024）京01民终2号",
    )
    existing_content = make_legacy_versions_page(
        title=original_title,
        court=court,
        entry_title=existing_case_title,
    )
    saves = []

    def fake_save_page(target_title, content, summary, **kwargs):
        saves.append((target_title, content, summary))
        return True

    monkeypatch.setattr(conflict_resolution, "save_page", fake_save_page)

    resolved, new_title, error = conflict_resolution.try_resolve_conflict(
        original_title=original_title,
        draft_content=draft_content,
        existing_content=existing_content,
    )

    assert resolved is True
    assert new_title == draft_case_title
    assert error is None
    assert saves[0][0] == original_title
    assert "{{裁判文书消歧义页" in saves[0][1]
    assert "{{versions" not in saves[0][1]
    assert "| court      = 北京市第一中级人民法院" in saves[0][1]
    assert "| type       = 民事判决书" in saves[0][1]
    assert "| year       = 2024" in saves[0][1]
    assert f"* [[{existing_case_title}]]" in saves[0][1]
    assert f"* [[{draft_case_title}]]" in saves[0][1]
    assert "[[Category:" not in saves[0][1]
    assert "转换为裁判文书消歧义页" in saves[0][2]


def test_try_resolve_conflict_converts_legacy_versions_page_even_when_entry_exists(monkeypatch):
    original_title = "共享标题"
    court = "北京市第一中级人民法院"
    case_title = "北京市第一中级人民法院（2024）京01民终1号民事判决书"
    draft_content = make_header_page(
        title=original_title,
        court=court,
        doc_type="民事判决书",
        case_number="（2024）京01民终1号",
    )
    existing_content = make_legacy_versions_page(
        title=original_title,
        court=court,
        entry_title=case_title,
    )
    saves = []

    def fake_save_page(target_title, content, summary, **kwargs):
        saves.append((target_title, content, summary))
        return True

    monkeypatch.setattr(conflict_resolution, "save_page", fake_save_page)

    resolved, new_title, error = conflict_resolution.try_resolve_conflict(
        original_title=original_title,
        draft_content=draft_content,
        existing_content=existing_content,
    )

    assert resolved is True
    assert new_title == case_title
    assert error is None
    assert saves[0][0] == original_title
    assert "{{裁判文书消歧义页" in saves[0][1]
    assert "{{versions" not in saves[0][1]
    assert f"* [[{case_title}]]" in saves[0][1]
    assert "[[Category:" not in saves[0][1]
    assert saves[0][2] == "转换为裁判文书消歧义页"


def test_try_resolve_conflict_replaces_redirect_target_with_existing_document(monkeypatch):
    original_title = "共享标题"
    court = "北京市第一中级人民法院"
    existing_case_title = "北京市第一中级人民法院（2024）京01民终1号民事判决书"
    draft_case_title = "北京市第一中级人民法院（2024）京01民终2号民事判决书"
    existing_content = make_header_page(
        title=original_title,
        court=court,
        doc_type="民事判决书",
        case_number="（2024）京01民终1号",
    )
    draft_content = make_header_page(
        title=original_title,
        court=court,
        doc_type="民事判决书",
        case_number="（2024）京01民终2号",
    )
    pages = {}
    saves = []
    moves = []

    def fake_resolve_page(requested_title):
        assert requested_title == existing_case_title
        return ResolvedPage(
            requested_title=requested_title,
            exists=True,
            is_redirect=True,
            redirect_target=original_title,
            resolved_title=original_title,
            content=existing_content,
        )

    def fake_save_page(target_title, content, summary, **kwargs):
        pages[target_title] = content
        saves.append((target_title, content, summary))
        return True

    def fake_move_page(from_title, to_title, reason="", leave_redirect=True, ignore_warnings=False):
        moves.append((from_title, to_title, reason, leave_redirect, ignore_warnings))
        pages[to_title] = existing_content
        return True

    monkeypatch.setattr(conflict_resolution, "resolve_page", fake_resolve_page)
    monkeypatch.setattr(conflict_resolution, "can_move_over_redirect", lambda from_title, to_title: True)
    monkeypatch.setattr(conflict_resolution, "move_page", fake_move_page)
    monkeypatch.setattr(conflict_resolution, "save_page", fake_save_page)
    monkeypatch.setattr(
        conflict_resolution,
        "get_page_content",
        lambda requested_title: (requested_title in pages, pages.get(requested_title)),
    )

    resolved, new_title, error = conflict_resolution.try_resolve_conflict(
        original_title=original_title,
        draft_content=draft_content,
        existing_content=existing_content,
    )

    assert resolved is True
    assert error is None
    assert new_title == draft_case_title
    assert moves == [
        (
            original_title,
            existing_case_title,
            f"移动至具体案号页面，原标题改为消歧义页：[[{original_title}]]",
            True,
            True,
        )
    ]
    assert saves[0][0] == existing_case_title
    assert "[[共享标题]]" in saves[0][1]
    assert saves[1][0] == original_title
    assert "{{裁判文书消歧义页" in saves[1][1]
    assert "| court      = 北京市第一中级人民法院" in saves[1][1]
    assert "| type       = 民事判决书" in saves[1][1]
    assert "| year       = 2024" in saves[1][1]
    assert "[[Category:" not in saves[1][1]


def test_process_upload_batch_prefetches_page_queries(tmp_path, monkeypatch):
    input_path = tmp_path / "input.jsonl"
    uploaded_log = tmp_path / "uploaded.jsonl"
    failed_log = tmp_path / "failed.jsonl"
    skipped_log = tmp_path / "skipped.jsonl"
    overwritable_log = tmp_path / "overwritable.jsonl"

    first_title = "张三与李四民事判决书"
    second_title = "王五与赵六民事判决书"
    first_case_title = "北京市第一中级人民法院（2024）京01民终1号民事判决书"
    second_case_title = "北京市第一中级人民法院（2024）京01民终2号民事判决书"
    first_wikitext = make_header_page(
        title=first_title,
        court="北京市第一中级人民法院",
        doc_type="民事判决书",
        case_number="（2024）京01民终1号",
    )
    second_wikitext = make_header_page(
        title=second_title,
        court="北京市第一中级人民法院",
        doc_type="民事判决书",
        case_number="（2024）京01民终2号",
    )

    input_path.write_text(
        "\n".join(
            [
                json.dumps({"title": first_title, "wenshu_id": "doc-1", "wikitext": first_wikitext}, ensure_ascii=False),
                json.dumps({"title": second_title, "wenshu_id": "doc-2", "wikitext": second_wikitext}, ensure_ascii=False),
            ]
        ),
        encoding="utf-8",
    )

    seen = {
        "title_batches": [],
        "case_batches": [],
        "upload_calls": [],
    }

    monkeypatch.setattr(uploader, "DEFAULT_UPLOAD_QUERY_BATCH_SIZE", 2)

    def fake_fetch_page_content_batch(titles, batch_size):
        seen["title_batches"].append((list(titles), batch_size))
        return {
            title: PageSnapshot(
                requested_title=title,
                exists=False,
                canonical_title=title,
                content=None,
            )
            for title in titles
        }

    def fake_resolve_pages_batch(titles, batch_size):
        seen["case_batches"].append((list(titles), batch_size))
        return {
            title: ResolvedPage(
                requested_title=title,
                exists=False,
            )
            for title in titles
        }

    def fake_upload_document(
        *,
        title,
        wenshu_id,
        wikitext,
        resolve_conflicts,
        force_overwrite,
        existing_page,
        case_page,
    ):
        seen["upload_calls"].append((title, wenshu_id, existing_page, case_page))
        assert existing_page is not None
        assert case_page is not None
        return uploader.UploadResult(
            title=title,
            wenshu_id=wenshu_id,
            status="uploaded",
            final_title=title,
            case_title=uploader.build_case_title_from_content(wikitext),
            message="Created successfully",
            timestamp=uploader.utc_now_iso(),
        )

    monkeypatch.setattr(uploader, "fetch_page_content_batch", fake_fetch_page_content_batch)
    monkeypatch.setattr(uploader, "resolve_pages_batch", fake_resolve_pages_batch)
    monkeypatch.setattr(uploader, "upload_document", fake_upload_document)

    uploaded, failed, skipped, resolved, overwritable = uploader.process_upload_batch(
        input_path=input_path,
        uploaded_log=uploaded_log,
        failed_log=failed_log,
        skipped_log=skipped_log,
        overwritable_log=overwritable_log,
    )

    assert (uploaded, failed, skipped, resolved, overwritable) == (2, 0, 0, 0, 0)
    assert seen["title_batches"] == [([first_title, second_title], 2)]
    assert seen["case_batches"] == [([first_case_title, second_case_title], 2)]
    assert [call[0] for call in seen["upload_calls"]] == [first_title, second_title]


def test_process_upload_batch_invalidates_stale_prefetch_after_write(tmp_path, monkeypatch):
    input_path = tmp_path / "input.jsonl"
    uploaded_log = tmp_path / "uploaded.jsonl"
    failed_log = tmp_path / "failed.jsonl"
    skipped_log = tmp_path / "skipped.jsonl"
    overwritable_log = tmp_path / "overwritable.jsonl"

    title = "张三与李四民事判决书"
    case_title = "北京市第一中级人民法院（2024）京01民终1号民事判决书"
    wikitext = make_header_page(
        title=title,
        court="北京市第一中级人民法院",
        doc_type="民事判决书",
        case_number="（2024）京01民终1号",
    )

    input_path.write_text(
        "\n".join(
            [
                json.dumps({"title": title, "wenshu_id": "doc-1", "wikitext": wikitext}, ensure_ascii=False),
                json.dumps({"title": title, "wenshu_id": "doc-2", "wikitext": wikitext}, ensure_ascii=False),
            ]
        ),
        encoding="utf-8",
    )

    wiki_pages: dict[str, str] = {}
    save_calls: list[str] = []

    monkeypatch.setattr(uploader, "DEFAULT_UPLOAD_QUERY_BATCH_SIZE", 2)

    def fake_fetch_page_content_batch(titles, batch_size):
        return {
            requested_title: PageSnapshot(
                requested_title=requested_title,
                exists=False,
                canonical_title=requested_title,
                content=None,
            )
            for requested_title in titles
        }

    def fake_resolve_pages_batch(titles, batch_size):
        return {
            requested_title: ResolvedPage(
                requested_title=requested_title,
                exists=False,
            )
            for requested_title in titles
        }

    def fake_check_page_exists(requested_title):
        return (requested_title in wiki_pages, 1 if requested_title in wiki_pages else None)

    def fake_get_page_content(requested_title):
        if requested_title in wiki_pages:
            return True, wiki_pages[requested_title]
        return False, None

    def fake_resolve_page(requested_title):
        if requested_title not in wiki_pages:
            return ResolvedPage(requested_title=requested_title, exists=False)

        content = wiki_pages[requested_title]
        redirect_target = extract_redirect_target(content)
        if redirect_target:
            return ResolvedPage(
                requested_title=requested_title,
                exists=True,
                is_redirect=True,
                redirect_target=redirect_target,
                resolved_title=redirect_target,
                content=wiki_pages.get(redirect_target),
            )

        return ResolvedPage(
            requested_title=requested_title,
            exists=True,
            resolved_title=requested_title,
            content=content,
        )

    def fake_save_page(target_title, content, summary, **kwargs):
        save_calls.append(target_title)
        wiki_pages[target_title] = content
        return True

    monkeypatch.setattr(uploader, "fetch_page_content_batch", fake_fetch_page_content_batch)
    monkeypatch.setattr(uploader, "resolve_pages_batch", fake_resolve_pages_batch)
    monkeypatch.setattr(uploader, "check_page_exists", fake_check_page_exists)
    monkeypatch.setattr(uploader, "get_page_content", fake_get_page_content)
    monkeypatch.setattr(uploader, "resolve_page", fake_resolve_page)
    monkeypatch.setattr(uploader, "save_page", fake_save_page)

    uploaded, failed, skipped, resolved, overwritable = uploader.process_upload_batch(
        input_path=input_path,
        uploaded_log=uploaded_log,
        failed_log=failed_log,
        skipped_log=skipped_log,
        overwritable_log=overwritable_log,
    )

    assert (uploaded, failed, skipped, resolved, overwritable) == (1, 0, 1, 0, 0)
    assert save_calls.count(title) == 1
    assert save_calls.count(case_title) == 1


def test_process_upload_batch_uses_fast_forwarding_label_while_skipping(tmp_path, monkeypatch):
    input_path = tmp_path / "input.jsonl"
    uploaded_log = tmp_path / "uploaded.jsonl"
    failed_log = tmp_path / "failed.jsonl"
    skipped_log = tmp_path / "skipped.jsonl"
    overwritable_log = tmp_path / "overwritable.jsonl"

    skipped_title = "甲案民事判决书"
    active_title = "乙案民事判决书"
    active_wikitext = make_header_page(
        title=active_title,
        court="北京市第一中级人民法院",
        doc_type="民事判决书",
        case_number="（2024）京01民终2号",
    )

    input_path.write_text(
        "\n".join(
            [
                json.dumps({"title": skipped_title, "wenshu_id": "skip-1", "wikitext": "ignored"}, ensure_ascii=False),
                json.dumps({"title": active_title, "wenshu_id": "doc-2", "wikitext": active_wikitext}, ensure_ascii=False),
            ]
        ),
        encoding="utf-8",
    )

    descriptions = []

    class FakeProgress:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def add_task(self, description, **kwargs):
            descriptions.append(description)
            return "task-1"

        def update(self, task_id, **kwargs):
            description = kwargs.get("description")
            if description is not None:
                descriptions.append(description)

    def fake_fetch_page_content_batch(titles, batch_size):
        return {
            title: PageSnapshot(
                requested_title=title,
                exists=False,
                canonical_title=title,
                content=None,
            )
            for title in titles
        }

    def fake_resolve_pages_batch(titles, batch_size):
        return {
            title: ResolvedPage(
                requested_title=title,
                exists=False,
            )
            for title in titles
        }

    def fake_upload_document(
        *,
        title,
        wenshu_id,
        wikitext,
        resolve_conflicts,
        force_overwrite,
        existing_page,
        case_page,
    ):
        return uploader.UploadResult(
            title=title,
            wenshu_id=wenshu_id,
            status="uploaded",
            final_title=title,
            case_title=uploader.build_case_title_from_content(wikitext),
            message="Created successfully",
            timestamp=uploader.utc_now_iso(),
        )

    monkeypatch.setattr(uploader, "Progress", FakeProgress)
    monkeypatch.setattr(uploader, "fetch_page_content_batch", fake_fetch_page_content_batch)
    monkeypatch.setattr(uploader, "resolve_pages_batch", fake_resolve_pages_batch)
    monkeypatch.setattr(uploader, "upload_document", fake_upload_document)

    uploaded, failed, skipped, resolved, overwritable = uploader.process_upload_batch(
        input_path=input_path,
        uploaded_log=uploaded_log,
        failed_log=failed_log,
        skipped_log=skipped_log,
        overwritable_log=overwritable_log,
        skip_lines=1,
    )

    assert (uploaded, failed, skipped, resolved, overwritable) == (1, 0, 0, 0, 0)
    assert descriptions[0].startswith("[cyan]Fast-forwarding:")
    assert any(description.startswith("[cyan]Uploading:") for description in descriptions[1:])
