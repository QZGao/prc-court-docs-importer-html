"""
Tests for upload-time redirect creation, batching, and conflict resolution.
"""

import json
from textwrap import dedent

from upload.mediawiki import PageSnapshot, ResolvedPage
from upload import uploader
from upload import conflict_resolution


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

    monkeypatch.setattr(conflict_resolution, "resolve_page", fake_resolve_page)
    monkeypatch.setattr(
        conflict_resolution,
        "move_page",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("move_page should not be called")),
    )
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
    assert saves[0][0] == existing_case_title
    assert saves[1][0] == existing_case_title
    assert "[[共享标题]]" in saves[1][1]
    assert saves[2][0] == original_title
    assert "{{versions" in saves[2][1]


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
