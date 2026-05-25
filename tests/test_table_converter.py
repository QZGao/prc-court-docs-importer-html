"""Tests for HTML table conversion."""

from pathlib import Path

import pytest

from convert.converter import convert_document, iter_json_objects
from convert.html_normalizer import BlockType, normalize_html
from convert.wikitext_renderer import convert_html_to_wikitext


TABLE_CASES_PATH = Path(__file__).with_name("table_test_cases.jsonl")


def load_table_cases():
    """Load compact table-bearing fixtures extracted from the large s41 JSONL."""
    with open(TABLE_CASES_PATH, "r", encoding="utf-8") as f:
        return list(iter_json_objects(f))


def to_raw_case(case: dict) -> dict:
    """Convert the compact fixture shape back to converter input shape."""
    return {
        "s1": case["title"],
        "wsKey": case["wsKey"],
        "s2": case["s2"],
        "s7": case["s7"],
        "s22": case["s22"],
        "s31": case["s31"],
        "qwContent": case["qwContent"],
    }


def table_blocks_from_doc(doc):
    return [
        block
        for block in doc.body_blocks + doc.signature_blocks + doc.further_notes
        if block.block_type == BlockType.TABLE
    ]


def test_table_converter_preserves_table_markup_features():
    html = """
    <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>江西省南昌市人民法院</div>
    <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>民事判决书</div>
    <div style='TEXT-ALIGN: right;'>（2024）赣01民初123号</div>
    <table class='source' border='1' cellpadding='2'>
        <caption style='text-align:center'>费用明细</caption>
        <tr style='background:#eee'><th colspan='2' scope='col'>表头</th></tr>
        <tr><td rowspan='2' style='width:4em'>甲</td><td>A | B<br>换行</td></tr>
        <tr><td>丙</td></tr>
    </table>
    <div style='TEXT-ALIGN: right;'>审判员　张三</div>
    <div style='TEXT-ALIGN: right;'>二〇二四年一月一日</div>
    """
    wikitext = convert_html_to_wikitext(html, "测试标题")

    assert '{| class="source wikitable" border="1" cellpadding="2"' in wikitext
    assert '|+ style="text-align:center" | 费用明细' in wikitext
    assert '|- style="background:#eee"' in wikitext
    assert '! colspan="2" scope="col" | 表头' in wikitext
    assert '| rowspan="2" style="width:4em" | 甲' in wikitext
    assert 'A | B\n换行' in wikitext


def test_table_converter_handles_nested_tables_inside_wrappers():
    html = """
    <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>江西省南昌市人民法院</div>
    <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>民事判决书</div>
    <div style='TEXT-ALIGN: right;'>（2024）赣01民初123号</div>
    <div>
        <p style='TEXT-INDENT: 30pt;'>正文前。</p>
        <table id='outer'>
            <tr>
                <td>外层
                    <table id='inner'>
                        <tr><th>内表头</th></tr>
                        <tr><td>内文</td></tr>
                    </table>
                </td>
            </tr>
        </table>
        <p style='TEXT-ALIGN: right;'>审判员　张三</p>
        <p style='TEXT-ALIGN: right;'>二〇二四年一月一日</p>
    </div>
    """
    wikitext = convert_html_to_wikitext(html, "测试标题")

    assert "{{gap}}正文前。" in wikitext
    assert '{| class="wikitable" id="outer"' in wikitext
    assert '{| class="wikitable" id="inner"' in wikitext
    assert "! 内表头" in wikitext
    assert "| 内文" in wikitext
    assert "外层内表头内文" not in wikitext


@pytest.mark.parametrize(
    "case",
    load_table_cases(),
    ids=lambda case: f"record-{case['source_index']}",
)
def test_extracted_s41_table_cases_are_structured_and_rendered(case):
    doc = normalize_html(case["qwContent"])
    table_blocks = table_blocks_from_doc(doc)

    assert len(table_blocks) == case["expected_table_count"]
    assert all(block.raw_html for block in table_blocks)
    assert all(block.metadata.get("html") for block in table_blocks)

    result, error = convert_document(to_raw_case(case))

    assert error is None
    assert result is not None

    wikitext = result.wikitext
    assert wikitext.count("{|") == case["expected_table_count"]
    assert wikitext.count("|}") == case["expected_table_count"]
    assert '<table' not in wikitext.lower()
    assert '</table>' not in wikitext.lower()
    assert '<td' not in wikitext.lower()
    assert '</td>' not in wikitext.lower()
    assert '{| class="wikitable" border="1" cellpadding="0" cellspacing="0" xmlns="http://www.w3.org/1999/xhtml"' in wikitext
    assert 'width="' in wikitext

    if "colspan=" in case["qwContent"].lower():
        assert 'colspan="' in wikitext
    if "rowspan=" in case["qwContent"].lower():
        assert 'rowspan="' in wikitext
