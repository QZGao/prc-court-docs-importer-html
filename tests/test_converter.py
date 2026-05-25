"""
Tests for the court document converter.

Run with: pytest tests/
"""

import json
import pytest
from pathlib import Path

# Add parent directory to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

import convert.__main__ as convert_cli
from convert.html_normalizer import (
    normalize_html,
    clean_text,
    is_date_text,
    is_signature_text,
    extract_date_components,
    normalize_redaction_markers,
    normalize_title_redaction_markers,
    remove_cjk_spaces,
    remove_unicode_other_chars,
)
from convert.wikitext_renderer import (
    convert_html_to_wikitext,
)
from convert.location import (
    infer_location_from_court,
    extract_province_from_court,
)
from upload.page_metadata import build_case_title_from_content
from convert.converter import (
    ConversionInterrupted,
    convert_document,
    extract_case_number_from_s22,
    extract_date_components_from_s31,
    extract_doc_type_from_s22,
    extract_doc_type_from_title,
    infer_court_with_province,
    iter_json_objects,
    normalize_case_number_value,
    normalize_court_name,
    normalize_doc_type,
    process_jsonl_stream,
)


# Load test cases
TEST_CASES_PATH = Path(__file__).parent / "test_cases.json"


def load_test_cases():
    """Load test cases using the converter's JSON reader."""
    with open(TEST_CASES_PATH, 'r', encoding='utf-8') as f:
        return list(iter_json_objects(f))


def make_raw_case(
    title: str,
    ws_key: str,
    doc_id: str,
    court: str = "北京市第一中级人民法院",
    doc_type: str = "民事判决书",
) -> dict:
    """Build a minimal raw record that successfully converts."""
    return {
        "s1": title,
        "wsKey": ws_key,
        "s2": court,
        "s7": doc_id,
        "s22": f"{court}\n{doc_type}\n{doc_id}",
        "qwContent": f"""
            <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>{court}</div>
            <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>{doc_type}</div>
            <div style='TEXT-ALIGN: right;'>{doc_id}</div>
            <div style='TEXT-INDENT: 30pt;'>正文内容。</div>
            <div style='TEXT-ALIGN: right;'>审判员　李四</div>
            <div style='TEXT-ALIGN: right;'>二〇二四年一月一日</div>
        """,
    }


def write_jsonl(path: Path, records: list[dict]) -> None:
    with open(path, 'w', encoding='utf-8') as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


class TestCleanText:
    """Tests for text cleaning function."""
    
    def test_basic_cleanup(self):
        assert clean_text("  hello  world  ") == "hello world"
    
    def test_newlines_removed(self):
        assert clean_text("hello\nworld") == "hello world"
    
    def test_tabs_removed(self):
        assert clean_text("hello\tworld") == "hello world"
    
    def test_empty_string(self):
        assert clean_text("") == ""
    
    def test_none_handling(self):
        assert clean_text(None) == ""

    def test_invisible_unicode_other_chars_removed(self):
        assert clean_text("张三\u200e李四\u200b") == "张三李四"


class TestUnicodeOtherNormalization:
    """Tests for invisible Unicode Other-category cleanup."""

    def test_format_chars_removed(self):
        assert remove_unicode_other_chars("甲\u200e乙\u200b丙\ufeff") == "甲乙丙"

    def test_structural_controls_preserved_for_later_normalization(self):
        assert remove_unicode_other_chars("法院\n判决书\t案号") == "法院\n判决书\t案号"


class TestRedactionNormalization:
    """Tests for repeated redaction marker normalization."""

    def test_two_markers_preserve_run_length(self):
        assert normalize_redaction_markers("张三xx李四") == "张三{{PRC-redact|2}}李四"
        assert normalize_redaction_markers("张三**李四") == "张三{{PRC-redact|2}}李四"

    def test_three_or_more_markers_preserve_run_length(self):
        assert normalize_redaction_markers("张三xxx李四") == "张三{{PRC-redact|3}}李四"
        assert normalize_redaction_markers("张三Xｘ*李四") == "张三{{PRC-redact|3}}李四"
        assert normalize_redaction_markers("张三Ｘ×xx李四") == "张三{{PRC-redact|4}}李四"

    def test_single_marker_is_normalized(self):
        assert normalize_redaction_markers("张三x李四") == "张三{{PRC-redact|1}}李四"
        assert normalize_redaction_markers("*") == "{{PRC-redact|1}}"

    def test_non_cross_markers_redact_next_to_numbers(self):
        assert normalize_redaction_markers("张三*123") == "张三{{PRC-redact|1}}123"
        assert normalize_redaction_markers("张三**123") == "张三{{PRC-redact|2}}123"
        assert normalize_redaction_markers("张三xx123") == "张三{{PRC-redact|2}}123"
        assert normalize_redaction_markers("3x123") == "3{{PRC-redact|1}}123"
        assert normalize_redaction_markers("{{PRC-redact|2}}*123") == "{{PRC-redact|3}}123"

    def test_literal_cross_number_adjacency_is_preserved(self):
        assert normalize_redaction_markers("3×123") == "3×123"
        assert normalize_redaction_markers("×123") == "×123"
        assert normalize_redaction_markers("1×号") == "1{{PRC-redact|1}}号"
        assert normalize_redaction_markers("××123") == "{{PRC-redact|2}}123"

    def test_latin_word_guard_still_applies_to_x_markers(self):
        assert normalize_redaction_markers("textXword") == "textXword"
        assert normalize_redaction_markers("textx") == "textx"
        assert normalize_redaction_markers("text*word") == "text{{PRC-redact|1}}word"

    def test_adjacent_x_markers_ignore_latin_word_guard(self):
        assert normalize_redaction_markers("textXXword") == "text{{PRC-redact|2}}word"
        assert normalize_redaction_markers("PLISXXXXXXXXXXXXX5108") == "PLIS{{PRC-redact|13}}5108"
        assert normalize_redaction_markers("PLISX5108") == "PLISX5108"


class TestTitleRedactionNormalization:
    """Tests for title-specific redaction normalization."""

    def test_title_uses_literal_multiplication_signs(self):
        assert normalize_title_redaction_markers("张三xxx执行裁定书") == "张三×××执行裁定书"
        assert normalize_title_redaction_markers("张三Xｘ*执行裁定书") == "张三×××执行裁定书"

    def test_title_single_marker_is_normalized(self):
        assert normalize_title_redaction_markers("张三x执行裁定书") == "张三×执行裁定书"
        assert normalize_title_redaction_markers("张三X执行裁定书") == "张三×执行裁定书"
        assert normalize_title_redaction_markers("张三*执行裁定书") == "张三×执行裁定书"
        assert normalize_title_redaction_markers("张三*123执行裁定书") == "张三×123执行裁定书"
        assert normalize_title_redaction_markers("3×123执行裁定书") == "3×123执行裁定书"
        assert normalize_title_redaction_markers("PLISXXXXXXXXXXXXX5108执行裁定书") == "PLIS×××××××××××××5108执行裁定书"


class TestRemoveCjkSpaces:
    """Tests for CJK space removal function."""
    
    def test_space_between_cjk_chars_removed(self):
        """Spaces between CJK characters should be removed."""
        assert remove_cjk_spaces("你 好") == "你好"
        assert remove_cjk_spaces("中 华 人 民 共 和 国") == "中华人民共和国"
    
    def test_multiple_spaces_between_cjk_removed(self):
        """Multiple consecutive spaces between CJK should all be removed."""
        assert remove_cjk_spaces("你  好") == "你好"
        assert remove_cjk_spaces("执 行 裁 定 书") == "执行裁定书"
    
    def test_space_between_cjk_and_latin_preserved(self):
        """Spaces between CJK and Latin characters should be preserved."""
        assert remove_cjk_spaces("使用 Python 编程") == "使用 Python 编程"
        assert remove_cjk_spaces("版本 2.0 发布") == "版本 2.0 发布"
    
    def test_space_between_cjk_and_number_preserved(self):
        """Spaces between CJK and numbers should be preserved."""
        assert remove_cjk_spaces("共 100 元") == "共 100 元"
        assert remove_cjk_spaces("第 3 条") == "第 3 条"
    
    def test_space_between_latin_chars_preserved(self):
        """Spaces between Latin characters should be preserved."""
        assert remove_cjk_spaces("hello world") == "hello world"
    
    def test_space_between_cjk_punctuation_removed(self):
        """Spaces between CJK punctuation and CJK characters should be removed."""
        assert remove_cjk_spaces("《 中华人民共和国民法典 》") == "《中华人民共和国民法典》"
        # Space between CJK punctuation and number is preserved (number is not CJK)
        assert remove_cjk_spaces("（ 2024 ）") == "（ 2024 ）"
        # Space between two CJK punctuation marks is removed
        assert remove_cjk_spaces("》 《") == "》《"
    
    def test_mixed_content(self):
        """Mixed CJK and Latin content should be handled correctly."""
        # OCR artifact: space in middle of Chinese text
        assert remove_cjk_spaces("向被执行人 发出执行通知书") == "向被执行人发出执行通知书"
        # Legitimate: space around English
        assert remove_cjk_spaces("使用 API 接口") == "使用 API 接口"
    
    def test_empty_string(self):
        """Empty string should return empty string."""
        assert remove_cjk_spaces("") == ""
    
    def test_none_returns_none(self):
        """None should return None (or empty based on implementation)."""
        assert remove_cjk_spaces(None) is None
    
    def test_no_spaces(self):
        """String with no spaces should be unchanged."""
        assert remove_cjk_spaces("中华人民共和国") == "中华人民共和国"
        assert remove_cjk_spaces("hello") == "hello"
    
    def test_real_ocr_artifacts(self):
        """Test with real OCR artifact patterns from court documents."""
        # These are actual patterns seen in the test data
        assert remove_cjk_spaces("在本案执 行过程中") == "在本案执行过程中"
        assert remove_cjk_spaces("被执行人名 下车牌号") == "被执行人名下车牌号"
        assert remove_cjk_spaces("本裁定送达后即发生法律效力 。") == "本裁定送达后即发生法律效力。"


class TestDateExtraction:
    """Tests for date-related functions."""
    
    def test_is_date_text_chinese(self):
        assert is_date_text("二〇二四年九月二十八日")
        assert is_date_text("二〇〇四年一月三十日")
    
    def test_is_date_text_arabic(self):
        assert is_date_text("2024年9月28日")
        assert is_date_text("2004年1月30日")
    
    def test_is_not_date_text(self):
        assert not is_date_text("审判员　张三")
        assert not is_date_text("本裁定送达后即发生法律效力")
    
    def test_extract_chinese_date(self):
        year, month, day = extract_date_components("二〇二四年九月二十八日")
        assert year == "2024"
        assert month == "9"
        assert day == "28"
    
    def test_extract_arabic_date(self):
        year, month, day = extract_date_components("2024年9月28日")
        assert year == "2024"
        assert month == "9"
        assert day == "28"

    def test_extract_s31_date(self):
        year, month, day = extract_date_components_from_s31("2014-09-30")
        assert year == "2014"
        assert month == "9"
        assert day == "30"

    def test_extract_s31_invalid_date_returns_empty_components(self):
        assert extract_date_components_from_s31("") == (None, None, None)
        assert extract_date_components_from_s31("2014/09/30") == (None, None, None)


class TestSignatureDetection:
    """Tests for signature text detection."""
    
    def test_judge_signature(self):
        assert is_signature_text("审判员　　章辉")
        assert is_signature_text("审 判 员　：杨清")
        assert is_signature_text("审判长　阿勒木江吾米尔江")
    
    def test_clerk_signature(self):
        assert is_signature_text("书记员　　魏凯")
        assert is_signature_text("书 记 员　曹　文")
        assert is_signature_text("........书记员刘敏")
    
    def test_assistant_signature(self):
        assert is_signature_text("法官助理　孙雨薇")
        assert is_signature_text("法官助理　阿丽吐古丽卡哈尔")
    
    def test_daili_prefix(self):
        """Test that 代理 prefix works for all positions."""
        assert is_signature_text("代理审判员　张三")
        assert is_signature_text("代理书记员　李四")
        assert is_signature_text("代理审判长　王五")
        assert is_signature_text("代理法官助理　赵六")
    
    def test_new_clerk_keywords(self):
        """Test the new clerk keywords."""
        assert is_signature_text("校对责任人　张三")
        assert is_signature_text("打印责任人　李四")
        assert is_signature_text("校对人　王五")
    
    def test_not_signature(self):
        assert not is_signature_text("本裁定送达后即发生法律效力")
        assert not is_signature_text("终结本次执行程序")
        assert not is_signature_text("本院于2021年4月13日由审判员周小兵担任审判长，依法公开开庭审理。")

    def test_dotted_date(self):
        assert is_date_text(".二〇二一年十二月七日")


class TestLocationInference:
    """Tests for location inference from court names."""
    
    def test_explicit_province(self):
        result = infer_location_from_court("江西省南昌市红谷滩区人民法院")
        assert result.startswith("江西省")
    
    def test_city_inference(self):
        result = infer_location_from_court("南昌市红谷滩区人民法院")
        assert "江西省" in result
    
    def test_inner_mongolia_flag(self):
        result = infer_location_from_court("鄂托克前旗人民法院")
        assert result == "内蒙古自治区"
    
    def test_xinjiang_court(self):
        result = infer_location_from_court("新疆维吾尔自治区昌吉回族自治州中级人民法院")
        assert result.startswith("新疆维吾尔自治区")
    
    def test_extract_province_only(self):
        assert extract_province_from_court("江西省南昌市红谷滩区人民法院") == "江西省"
        assert extract_province_from_court("南昌市红谷滩区人民法院") == "江西省"


class TestHtmlNormalization:
    """Tests for HTML parsing and normalization."""
    
    def test_basic_parsing(self):
        html = """
        <div style='TEXT-ALIGN: center;'>某某人民法院</div>
        <div style='TEXT-ALIGN: center;'>裁定书</div>
        <div style='TEXT-ALIGN: right;'>（2024）案号</div>
        """
        doc = normalize_html(html)
        assert doc.court_name == "某某人民法院"
        assert doc.doc_type == "裁定书"
    
    def test_paragraph_extraction(self):
        html = """
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>法院</div>
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>裁定书</div>
        <div style='TEXT-ALIGN: right;'>（2024）号</div>
        <div style='TEXT-INDENT: 30pt;'>这是正文内容。</div>
        """
        doc = normalize_html(html)
        assert len(doc.body_blocks) >= 1
        # Find the body paragraph
        body_texts = [b.text for b in doc.body_blocks]
        assert any("正文" in t for t in body_texts)

    def test_clean_text_redacts_non_cross_stars_next_to_numbers(self):
        text = "赔偿3×123元（4,942.28元ｘ70%），物业费2.3元/月.平方米*163.64平方米*48个月，张三*。"

        result = clean_text(text)

        assert "3×123元" in result
        assert "4,942.28元{{PRC-redact|1}}70%" in result
        assert "2.3元/月.平方米{{PRC-redact|1}}163.64平方米{{PRC-redact|1}}48个月" in result
        assert "张三{{PRC-redact|1}}。" in result


class TestDocTypeExtraction:
    """Tests for document type extraction from s22."""
    
    def test_basic_extraction(self):
        s22 = "江西省南昌市红谷滩区人民法院\n执行裁定书\n（2022）赣0113执6828号之一"
        result = extract_doc_type_from_s22(s22, "南昌市红谷滩区人民法院", "（2022）赣0113执6828号之一")
        assert result == "执行裁定书"
    
    def test_civil_judgment(self):
        s22 = "新疆维吾尔自治区昌吉回族自治州中级人民法院\n民事判决书\n（2024）新23民终1709号"
        result = extract_doc_type_from_s22(s22, "法院", "号")
        assert result == "民事判决书"

    def test_extraction_removes_deduced_court_and_case_number(self):
        s22 = "No.001 柳州市柳南区人民法院\n执行裁定书\n（2019）桂0204民初2440号附件"
        result = extract_doc_type_from_s22(s22, "柳州市柳南区人民法院", "（2019）桂0204民初2440号")
        assert result == "执行裁定书"

    def test_extraction_returns_empty_when_s22_has_only_court_and_case_number(self):
        s22 = "柳州市柳南区人民法院\n（2019）桂0204民初2440号"
        result = extract_doc_type_from_s22(s22, "柳州市柳南区人民法院", "（2019）桂0204民初2440号")
        assert result == ""

    def test_extraction_preserves_type_after_suffixed_case_number(self):
        s22 = "柳州市柳南区人民法院\n（2019）桂0204民初2440号之一\n执行裁定书"
        result = extract_doc_type_from_s22(s22, "柳州市柳南区人民法院", "（2019）桂0204民初2440号")
        assert result == "执行裁定书"

    @pytest.mark.parametrize("doc_type", ["民事判决书", "刑事判决书", "行政判决书", "民事令", "刑事令", "行政令"])
    def test_title_suffix_extraction(self, doc_type):
        assert extract_doc_type_from_title(f"张三与李四纠纷{doc_type}") == doc_type

    @pytest.mark.parametrize("doc_type", ["民事裁定书", "执行裁定书", "民事调解书", "结案通知书", "执行通知书", "支付令"])
    def test_title_suffix_extraction_observed_s41_suffixes(self, doc_type):
        assert extract_doc_type_from_title(f"张三与李四纠纷{doc_type}") == doc_type

    @pytest.mark.parametrize("doc_type", ["刑事附带民事判决书", "民事支付令", "行政赔偿判决书"])
    def test_title_suffix_extraction_prefers_longest_suffix(self, doc_type):
        assert extract_doc_type_from_title(f"张三与李四纠纷{doc_type}") == doc_type

    def test_title_suffix_extraction_ignores_unlisted_suffixes(self):
        assert extract_doc_type_from_title("测试结案报告") == ""


class TestCourtInference:
    """Tests for court name inference with province."""

    def test_normalize_court_name_removes_leading_non_cjk_junk(self):
        assert normalize_court_name("  12.【x】北京市第一中级人民法院") == "北京市第一中级人民法院"

    def test_normalize_court_name_removes_trailing_junk_after_fayuan(self):
        assert normalize_court_name("北京市第一中级人民法院（执行局）") == "北京市第一中级人民法院"

    def test_normalize_court_name_keeps_immediate_committee_suffix(self):
        assert normalize_court_name("四川省绵阳市中级人民法院赔偿委员会") == "四川省绵阳市中级人民法院赔偿委员会"

    def test_normalize_court_name_strips_metadata_after_committee_suffix(self):
        s22 = "四川省绵阳市中级人民法院赔偿委员会\n国家赔偿决定书\n（2021）川07委赔8号"
        assert normalize_court_name(s22) == "四川省绵阳市中级人民法院赔偿委员会"

    def test_extract_doc_type_from_s22_removes_committee_court_name(self):
        s22 = "四川省绵阳市中级人民法院赔偿委员会\n国家赔偿决定书\n（2021）川07委赔8号"
        result = extract_doc_type_from_s22(s22, "四川省绵阳市中级人民法院赔偿委员会", "（2021）川07委赔8号")
        assert result == "国家赔偿决定书"

    def test_normalize_case_number_extracts_parenthesized_number_through_hao(self):
        assert normalize_case_number_value("案号：(2024)京01执123号附件") == "（2024）京01执123号"

    def test_normalize_case_number_keeps_zhi_suffix(self):
        assert normalize_case_number_value("案号：(2024)京01执123号之一附件") == "（2024）京01执123号之一"

    def test_normalize_doc_type_keeps_only_cjk(self):
        assert normalize_doc_type("No. 执 行 裁 定 书 123") == "执行裁定书"

    def test_extract_case_number_from_s22(self):
        assert extract_case_number_from_s22("柳州市柳南区人民法院\n（2019）桂0204民初2440号") == "（2019）桂0204民初2440号"

    def test_extract_case_number_from_s22_keeps_zhi_suffix(self):
        assert extract_case_number_from_s22("柳州市柳南区人民法院\n（2019）桂0204民初2440号之一") == "（2019）桂0204民初2440号之一"

    def test_from_s22(self):
        s22 = "江西省南昌市红谷滩区人民法院\n执行裁定书\n（2022）赣0113执6828号之一"
        result = infer_court_with_province("南昌市红谷滩区人民法院", s22)
        assert result == "江西省南昌市红谷滩区人民法院"
    
    def test_fallback_to_s2(self):
        result = infer_court_with_province("南昌市红谷滩区人民法院", "")
        assert result == "南昌市红谷滩区人民法院"

    def test_inference_removes_leading_non_cjk_junk(self):
        s22 = "No.001 北京市第一中级人民法院\n执行裁定书\n（2024）京01执123号"
        result = infer_court_with_province("x北京市第一中级人民法院", s22)
        assert result == "北京市第一中级人民法院"


class TestFullConversion:
    """Integration tests using real test cases."""
    
    @pytest.fixture
    def test_cases(self):
        return load_test_cases()
    
    def test_conversion_does_not_crash(self, test_cases):
        """Ensure conversion doesn't crash on any test case."""
        for case in test_cases:
            result, error = convert_document(case)
            # Should either succeed or return a proper error
            assert result is not None or error is not None
    
    def test_successful_conversion_has_required_fields(self, test_cases):
        """Successful conversions should have all required fields."""
        for case in test_cases:
            result, error = convert_document(case)
            if result:
                assert result.title
                assert result.wikitext
                assert result.court
    
    def test_wikitext_has_header(self, test_cases):
        """Generated wikitext should contain {{Header/裁判文书}} template."""
        for case in test_cases:
            result, error = convert_document(case)
            if result:
                assert "{{Header/裁判文书" in result.wikitext
    
    def test_wikitext_has_pd_template(self, test_cases):
        """Generated wikitext should contain PD template."""
        for case in test_cases:
            result, error = convert_document(case)
            if result:
                assert "{{PD-PRC-exempt}}" in result.wikitext


class TestWikitextRendering:
    """Tests for wikitext output format."""
    
    def test_header_template_format(self):
        """Test that header template is properly formatted."""
        html = """
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>江西省南昌市人民法院</div>
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>执行裁定书</div>
        <div style='TEXT-ALIGN: right;'>（2024）赣01执123号</div>
        <div style='TEXT-INDENT: 30pt;'>正文内容。</div>
        <div style='TEXT-ALIGN: right;'>审判员　张三</div>
        <div style='TEXT-ALIGN: right;'>二〇二四年一月一日</div>
        <div style='TEXT-ALIGN: right;'>书记员　李四</div>
        """
        wikitext = convert_html_to_wikitext(html, "测试标题")
        
        # Check header template
        assert "|title = 测试标题" in wikitext
        assert "|court = 江西省南昌市人民法院" in wikitext
        assert "|type = 执行裁定书" in wikitext
        assert "|案号 = （2024）赣01执123号" in wikitext

    def test_header_template_normalizes_halfwidth_case_number_parentheses(self):
        html = """
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>江西省南昌市人民法院</div>
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>执行裁定书</div>
        <div style='TEXT-ALIGN: right;'>(2024)赣01执123号</div>
        <div style='TEXT-INDENT: 30pt;'>正文内容。</div>
        <div style='TEXT-ALIGN: right;'>审判员　张三</div>
        <div style='TEXT-ALIGN: right;'>二〇二四年一月一日</div>
        """
        wikitext = convert_html_to_wikitext(html, "测试标题")

        assert "|案号 = （2024）赣01执123号" in wikitext
    
    def test_signature_template_format(self):
        """Test that {{裁判文书署名}} template is used for signatures."""
        html = """
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>北京市高级人民法院</div>
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>民事判决书</div>
        <div style='TEXT-ALIGN: right;'>（2024）京民终1号</div>
        <div style='TEXT-INDENT: 30pt;'>正文。</div>
        <div style='TEXT-ALIGN: right;'>审判员　张三</div>
        <div style='TEXT-ALIGN: right;'>二〇二四年一月一日</div>
        """
        wikitext = convert_html_to_wikitext(html, "标题")

        assert "{{裁判文书署名|1=" in wikitext
        assert "审判员：张三" in wikitext

    def test_dotted_unaligned_signature_lines_are_extracted(self):
        html = """
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>四川省遂宁市船山区人民法院</div>
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>刑事判决书</div>
        <div style='TEXT-ALIGN: right;'>（2021）川0903刑初554号</div>
        <div style='TEXT-INDENT: 30pt;'>如不服本判决，可在接到判决书的第二日起十日内提出上诉。</div>
        <div style='TEXT-INDENT: 30pt;'>.审判长樊平</div>
        <div style='TEXT-INDENT: 30pt;'>.人民陪审员曾长清</div>
        <div style='TEXT-INDENT: 30pt;'>...人民陪审员王亚君</div>
        <div style='TEXT-INDENT: 30pt;'>..二〇二一年十一月三十日</div>
        <div style='TEXT-INDENT: 30pt;'>........书记员刘敏</div>
        <div style='TEXT-INDENT: 30pt;'>附：本案适用的相关法律条文</div>
        """

        wikitext = convert_html_to_wikitext(html, "测试标题")

        assert "审判长：樊平" in wikitext
        assert "人民陪审员：曾长清" in wikitext
        assert "人民陪审员：王亚君" in wikitext
        assert "二〇二一年十一月三十日" in wikitext
        assert "书记员：刘敏" in wikitext
        assert ".审判长" not in wikitext
        assert "{{裁判文书署名|1=\n}}\n" not in wikitext
        assert "{{gap}}附：本案适用的相关法律条文" in wikitext

    def test_signature_detection_starts_without_visible_case_number(self):
        html = """
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>四川省遂宁市船山区人民法院</div>
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>刑事判决书</div>
        <div style='TEXT-INDENT: 30pt;'>如不服本判决，可在接到判决书的第二日起十日内提出上诉。</div>
        <div style='TEXT-INDENT: 30pt;'>.审判员樊平</div>
        <div style='TEXT-INDENT: 30pt;'>.二〇二一年十二月七日</div>
        <div style='TEXT-INDENT: 30pt;'>书记员刘敏</div>
        <div style='TEXT-INDENT: 30pt;'>附：本案适用的相关法律条文</div>
        """

        wikitext = convert_html_to_wikitext(html, "测试标题")

        assert "审判员：樊平" in wikitext
        assert "书记员：刘敏" in wikitext
        assert "{{gap}}.审判员樊平" not in wikitext
        assert "{{gap}}附：本案适用的相关法律条文" in wikitext

    def test_dotted_right_aligned_case_number_is_not_signature_start(self):
        html = """
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>四川省遂宁市船山区人民法院</div>
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>刑事判决书</div>
        <div style='TEXT-ALIGN: right;'>..（2021）川0903刑初554号</div>
        <div style='TEXT-INDENT: 30pt;'>公诉机关遂宁市船山区人民检察院。</div>
        <div style='TEXT-INDENT: 30pt;'>如不服本判决，可在接到判决书的第二日起十日内提出上诉。</div>
        <div style='TEXT-INDENT: 30pt;'>.审判员樊平</div>
        <div style='TEXT-INDENT: 30pt;'>.二〇二一年十二月七日</div>
        <div style='TEXT-INDENT: 30pt;'>书记员刘敏</div>
        """

        doc = normalize_html(html)
        wikitext = convert_html_to_wikitext(html, "测试标题")

        assert doc.doc_id == "（2021）川0903刑初554号"
        assert "{{gap}}公诉机关遂宁市船山区人民检察院。" in wikitext
        assert "审判员：樊平" in wikitext
        assert "（2021）川0903刑初554号\n公诉机关" not in wikitext

    def test_merged_signature_roles_are_split(self):
        html = """
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>内蒙古自治区准格尔旗人民法院</div>
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>民事判决书</div>
        <div style='TEXT-ALIGN: right;'>（2021）内0622民初43号</div>
        <div style='TEXT-INDENT: 30pt;'>如不服本判决，可以上诉。</div>
        <div style='TEXT-ALIGN: right;'>审 判 员　　张　　 悦　　 蓉</div>
        <div style='TEXT-ALIGN: right;'>二〇二一年三月二十九日</div>
        <div style='TEXT-ALIGN: right;'>法官助理　　陈彦儒书记员云耀</div>
        """

        wikitext = convert_html_to_wikitext(html, "测试标题")

        assert "审判员：张悦蓉" in wikitext
        assert "法官助理：陈彦儒" in wikitext
        assert "书记员：云耀" in wikitext
        assert "陈彦儒书记员云耀" not in wikitext

    def test_body_renderer_merges_obvious_line_wrapped_paragraphs(self):
        html = """
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>安徽省阜南县人民法院</div>
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>民事判决书</div>
        <div style='TEXT-ALIGN: right;'>（2018）皖1225民初6139号</div>
        <div style='TEXT-INDENT: 30pt;'>第二百零六条借款人应当按照约定的期限返还借款。贷款</div>
        <div style='TEXT-INDENT: 30pt;'>人可以催告借款人在合理期限内返还。</div>
        <div style='TEXT-ALIGN: right;'>审判员　张三</div>
        """

        wikitext = convert_html_to_wikitext(html, "测试标题")

        assert "{{gap}}第二百零六条借款人应当按照约定的期限返还借款。贷款人可以催告借款人在合理期限内返还。" in wikitext
        assert "贷款\n\n{{gap}}人" not in wikitext

    def test_body_renderer_keeps_party_labels_separate_when_previous_line_lacks_punctuation(self):
        html = """
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>内蒙古自治区赤峰市松山区人民法院</div>
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>民事判决书</div>
        <div style='TEXT-ALIGN: right;'>（2022）内0404民初4954号</div>
        <div style='TEXT-INDENT: 30pt;'>被告：赤峰励为建筑劳务有限公司。住所地：内蒙古自治区赤峰市松山区</div>
        <div style='TEXT-INDENT: 30pt;'>法定代表人：朱云明，经理。</div>
        <div style='TEXT-ALIGN: right;'>审判员　张三</div>
        """

        wikitext = convert_html_to_wikitext(html, "测试标题")

        assert "{{gap}}被告：赤峰励为建筑劳务有限公司。住所地：内蒙古自治区赤峰市松山区\n\n{{gap}}法定代表人：朱云明，经理。" in wikitext
        assert "松山区法定代表人" not in wikitext

    def test_further_notes_renderer_merges_obvious_line_wrapped_paragraphs(self):
        html = """
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>安徽省阜南县人民法院</div>
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>民事判决书</div>
        <div style='TEXT-ALIGN: right;'>（2018）皖1225民初6139号</div>
        <div style='TEXT-INDENT: 30pt;'>正文。</div>
        <div style='TEXT-ALIGN: right;'>二〇一八年十一月十九日</div>
        <div style='TEXT-INDENT: 30pt;'>第二百零六条借款人应当按照约定的期限返还借款。贷款</div>
        <div style='TEXT-INDENT: 30pt;'>人可以催告借款人在合理期限内返还。</div>
        """

        wikitext = convert_html_to_wikitext(html, "测试标题")

        assert "{{gap}}第二百零六条借款人应当按照约定的期限返还借款。贷款人可以催告借款人在合理期限内返还。" in wikitext
        assert "贷款\n\n{{gap}}人" not in wikitext

    def test_body_renderer_indents_unstyled_colon_heading(self):
        html = """
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>四川省筠连县人民法院</div>
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>刑事判决书</div>
        <div style='TEXT-ALIGN: right;'>（2021）川1527刑初145号</div>
        <div>筠连县人民检察院指控：</div>
        <div style='TEXT-INDENT: 30pt;'>被告人潘某实施犯罪。</div>
        <div style='TEXT-ALIGN: right;'>审判员　张三</div>
        """

        wikitext = convert_html_to_wikitext(html, "测试标题")

        assert "{{gap}}筠连县人民检察院指控：" in wikitext
        assert "\n筠连县人民检察院指控：" not in wikitext

    def test_convert_document_redaction_normalizes_title_and_body(self):
        raw_json = {
            "s1": "关于张三xxx执行裁定书",
            "wsKey": "doc-1",
            "s2": "北京市第一中级人民法院",
            "s7": "（2024）京01执xx号",
            "s22": "北京市第一中级人民法院\n执行裁定书\n（2024）京01执xx号",
            "qwContent": """
                <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>北京市第一中级人民法院</div>
                <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>执行裁定书</div>
                <div style='TEXT-ALIGN: right;'>（2024）京01执xx号</div>
                <div style='TEXT-INDENT: 30pt;'>申请执行人张三ＸＸＸ。</div>
                <div style='TEXT-ALIGN: right;'>审判员　李四</div>
                <div style='TEXT-ALIGN: right;'>二〇二四年一月一日</div>
            """,
        }

        result, error = convert_document(raw_json)

        assert error is None
        assert result is not None
        assert result.title == "关于张三×××执行裁定书"
        assert "申请执行人张三{{PRC-redact|3}}。" in result.wikitext
        assert "|title = 关于张三×××执行裁定书" in result.wikitext
        assert "|案号 = （2024）京01执{{PRC-redact|2}}号" in result.wikitext

    def test_convert_document_normalizes_halfwidth_case_number_parentheses(self):
        raw_json = {
            "s1": "测试执行裁定书",
            "wsKey": "doc-2",
            "s2": "北京市第一中级人民法院",
            "s7": "(2024)京01执123号",
            "s22": "北京市第一中级人民法院\n执行裁定书\n(2024)京01执123号",
            "qwContent": """
                <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>北京市第一中级人民法院</div>
                <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>执行裁定书</div>
                <div style='TEXT-ALIGN: right;'>(2024)京01执123号</div>
                <div style='TEXT-INDENT: 30pt;'>正文。</div>
                <div style='TEXT-ALIGN: right;'>审判员　李四</div>
                <div style='TEXT-ALIGN: right;'>二〇二四年一月一日</div>
            """,
        }

        result, error = convert_document(raw_json)

        assert error is None
        assert result is not None
        assert result.doc_id == "（2024）京01执123号"
        assert "|案号 = （2024）京01执123号" in result.wikitext

    def test_convert_document_removes_invisible_unicode_other_chars(self):
        raw_json = {
            "s1": "测试\u200e执行裁定书",
            "wsKey": "doc\u200e-3",
            "s2": "北京市第一中级\u200e人民法院",
            "s7": "（2024）京01执\u200e123号",
            "s22": "北京市第一中级\u200e人民法院\n执行裁定书\n（2024）京01执\u200e123号",
            "qwContent": """
                <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>北京市第一中级\u200e人民法院</div>
                <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>执行裁定书</div>
                <div style='TEXT-ALIGN: right;'>（2024）京01执\u200e123号</div>
                <div style='TEXT-INDENT: 30pt;'>正文\u200b内容。</div>
                <div style='TEXT-ALIGN: right;'>审判员　李四</div>
                <div style='TEXT-ALIGN: right;'>二〇二四年一月一日</div>
            """,
        }

        result, error = convert_document(raw_json)

        assert error is None
        assert result is not None
        assert result.title == "测试执行裁定书"
        assert result.court == "北京市第一中级人民法院"
        assert result.doc_id == "（2024）京01执123号"
        assert "\u200e" not in result.wikitext
        assert "\u200b" not in result.wikitext
        assert result.wenshu_id == "doc-3"
        assert "|docid = doc-3" in result.wikitext
        assert "|court = 北京市第一中级人民法院" in result.wikitext
        assert "|案号 = （2024）京01执123号" in result.wikitext
        assert "正文内容。" in result.wikitext

    def test_convert_document_uses_metadata_fallbacks_for_header(self):
        raw_json = {
            "s1": "测试执行裁定书",
            "wsKey": "doc-4",
            "s2": "北京市第一中级人民法院",
            "s7": "(2024)京01执123号",
            "s31": "2024-09-30",
            "s22": "北京市第一中级人民法院\n执行裁定书\n(2024)京01执123号",
            "qwContent": """
                <div style='TEXT-INDENT: 30pt;'>正文。</div>
                <div style='TEXT-ALIGN: right;'>审判员　李四</div>
            """,
        }

        result, error = convert_document(raw_json)

        assert error is None
        assert result is not None
        assert result.court == "北京市第一中级人民法院"
        assert result.doc_type == "执行裁定书"
        assert result.doc_id == "（2024）京01执123号"
        assert "|court = 北京市第一中级人民法院" in result.wikitext
        assert "|type = 执行裁定书" in result.wikitext
        assert "|案号 = （2024）京01执123号" in result.wikitext
        assert "|year = 2024" in result.wikitext
        assert "|month = 9" in result.wikitext
        assert "|day = 30" in result.wikitext

    def test_convert_document_strips_leading_junk_from_parsed_court_everywhere(self):
        raw_json = {
            "s1": "测试执行裁定书",
            "wsKey": "doc-6",
            "s2": "北京市第一中级人民法院",
            "s7": "（2024）京01执123号",
            "s22": "北京市第一中级人民法院\n执行裁定书\n（2024）京01执123号",
            "qwContent": """
                <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>No.001 北京市第一中级人民法院（执行局）</div>
                <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>No.执行裁定书123</div>
                <div style='TEXT-ALIGN: right;'>（2024）京01执123号附件</div>
                <div style='TEXT-INDENT: 30pt;'>正文。</div>
                <div style='TEXT-ALIGN: right;'>审判员　李四</div>
                <div style='TEXT-ALIGN: right;'>二〇二四年一月一日</div>
            """,
        }

        result, error = convert_document(raw_json)

        assert error is None
        assert result is not None
        assert result.court == "北京市第一中级人民法院"
        assert result.doc_type == "执行裁定书"
        assert result.doc_id == "（2024）京01执123号"
        assert "|court = 北京市第一中级人民法院" in result.wikitext
        assert "|type = 执行裁定书" in result.wikitext
        assert "|案号 = （2024）京01执123号" in result.wikitext
        assert build_case_title_from_content(result.wikitext) == "北京市第一中级人民法院（2024）京01执123号执行裁定书"
        assert "No.001" not in result.wikitext
        assert "附件" not in result.wikitext

    def test_convert_document_strips_leading_junk_from_fallback_court_everywhere(self):
        raw_json = {
            "s1": "测试执行裁定书",
            "wsKey": "doc-7",
            "s2": "x北京市第一中级人民法院",
            "s7": "（2024）京01执123号",
            "s22": "No.001 北京市第一中级人民法院\n执行裁定书\n（2024）京01执123号",
            "s31": "2024-09-30",
            "qwContent": """
                <div style='TEXT-INDENT: 30pt;'>正文。</div>
                <div style='TEXT-ALIGN: right;'>审判员　李四</div>
            """,
        }

        result, error = convert_document(raw_json)

        assert error is None
        assert result is not None
        assert result.court == "北京市第一中级人民法院"
        assert "|court = 北京市第一中级人民法院" in result.wikitext
        assert build_case_title_from_content(result.wikitext) == "北京市第一中级人民法院（2024）京01执123号执行裁定书"
        assert "No.001" not in result.wikitext

    def test_convert_document_extracts_type_after_court_and_case_fallbacks(self):
        raw_json = {
            "s1": "测试执行裁定书",
            "wsKey": "doc-8",
            "s2": "柳州市柳南区人民法院",
            "s7": "（2019）桂0204民初2440号",
            "s22": "柳州市柳南区人民法院\n（2019）桂0204民初2440号",
            "s31": "2019-09-30",
            "qwContent": """
                <div style='TEXT-INDENT: 30pt;'>正文。</div>
                <div style='TEXT-ALIGN: right;'>审判员　李四</div>
            """,
        }

        result, error = convert_document(raw_json)

        assert error is None
        assert result is not None
        assert result.court == "柳州市柳南区人民法院"
        assert result.doc_id == "（2019）桂0204民初2440号"
        assert result.doc_type == "执行裁定书"

    def test_convert_document_extracts_doc_type_from_title_suffix(self):
        raw_json = {
            "s1": "张三危险驾驶罪刑事判决书",
            "wsKey": "doc-title-type",
            "s2": "柳州市柳南区人民法院",
            "s7": "（2019）桂0204刑初2440号",
            "s22": "柳州市柳南区人民法院\n（2019）桂0204刑初2440号",
            "s31": "2019-09-30",
            "qwContent": """
                <div style='TEXT-INDENT: 30pt;'>正文。</div>
                <div style='TEXT-ALIGN: right;'>审判员　李四</div>
            """,
        }

        result, error = convert_document(raw_json)

        assert error is None
        assert result is not None
        assert result.doc_type == "刑事判决书"
        assert "|type = 刑事判决书" in result.wikitext

    def test_convert_document_keeps_zhi_suffix_in_case_number(self):
        raw_json = {
            "s1": "测试执行裁定书",
            "wsKey": "doc-9",
            "s2": "柳州市柳南区人民法院",
            "s7": "（2019）桂0204民初2440号之一",
            "s22": "柳州市柳南区人民法院\n执行裁定书\n（2019）桂0204民初2440号之一",
            "s31": "2019-09-30",
            "qwContent": """
                <div style='TEXT-INDENT: 30pt;'>正文。</div>
                <div style='TEXT-ALIGN: right;'>审判员　李四</div>
            """,
        }

        result, error = convert_document(raw_json)

        assert error is None
        assert result is not None
        assert result.doc_id == "（2019）桂0204民初2440号之一"
        assert "|案号 = （2019）桂0204民初2440号之一" in result.wikitext
        assert build_case_title_from_content(result.wikitext) == "柳州市柳南区人民法院（2019）桂0204民初2440号之一执行裁定书"

    def test_convert_document_prefers_parsed_html_date_over_s31(self):
        raw_json = {
            "s1": "测试执行裁定书",
            "wsKey": "doc-5",
            "s2": "北京市第一中级人民法院",
            "s7": "（2024）京01执123号",
            "s31": "2024-09-30",
            "s22": "北京市第一中级人民法院\n执行裁定书\n（2024）京01执123号",
            "qwContent": """
                <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>北京市第一中级人民法院</div>
                <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>执行裁定书</div>
                <div style='TEXT-ALIGN: right;'>（2024）京01执123号</div>
                <div style='TEXT-INDENT: 30pt;'>正文。</div>
                <div style='TEXT-ALIGN: right;'>审判员　李四</div>
                <div style='TEXT-ALIGN: right;'>二〇二四年一月一日</div>
            """,
        }

        result, error = convert_document(raw_json)

        assert error is None
        assert result is not None
        assert "|year = 2024" in result.wikitext
        assert "|month = 1" in result.wikitext
        assert "|day = 1" in result.wikitext


class TestTestCasesConversion:
    """Test conversion of all test cases using the actual pipeline."""
    
    def test_convert_all_test_cases(self):
        """Run the actual converter pipeline on test cases."""
        import tempfile
        import os
        
        input_path = TEST_CASES_PATH
        output_path = Path(__file__).parent / "test_output.jsonl"
        error_path = Path(__file__).parent / "test_errors.jsonl"
        wikitext_path = Path(__file__).parent / "test_output.txt"
        
        # Clean up previous output files
        for p in [output_path, error_path]:
            if p.exists():
                p.unlink()
        
        # Run the actual converter
        success_count, error_count, skipped_count, last_doc_num = process_jsonl_stream(input_path, output_path, error_path)
        
        print(f"\nConverter results: {success_count} succeeded, {error_count} failed")
        print(f"Output: {output_path}")
        print(f"Errors: {error_path}")
        
        # Write human-readable wikitext output for review
        with open(wikitext_path, 'w', encoding='utf-8') as out:
            out.write("=" * 80 + "\n")
            out.write("CONVERTED WIKITEXT OUTPUT\n")
            out.write("=" * 80 + "\n")
            
            with open(output_path, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f, 1):
                    result = json.loads(line)
                    out.write(f"\n{'─' * 80}\n")
                    out.write(f"Case {i}: {result.get('title', 'Unknown')}\n")
                    out.write(f"{'─' * 80}\n\n")
                    out.write(result.get('wikitext', ''))
                    out.write("\n")
            
            out.write(f"\n{'=' * 80}\n")
            out.write(f"SUMMARY: {success_count} succeeded, {error_count} failed\n")
            out.write(f"{'=' * 80}\n")
        
        print(f"Wikitext: {wikitext_path}")
        
        # Test passes if we processed all documents
        test_cases = load_test_cases()
        assert success_count + error_count == len(test_cases)


class TestCheckpointBehavior:
    def test_process_jsonl_stream_overwrites_existing_outputs_on_fresh_run(self, tmp_path):
        input_path = tmp_path / "input.jsonl"
        output_path = tmp_path / "output.jsonl"
        error_path = tmp_path / "errors.jsonl"

        write_jsonl(input_path, [
            make_raw_case("甲案民事判决书", "doc-1", "（2024）京01民初1号"),
            make_raw_case("乙案民事判决书", "doc-2", "（2024）京01民初2号"),
        ])
        output_path.write_text("stale-output\n", encoding='utf-8')
        error_path.write_text("stale-error\n", encoding='utf-8')

        success_count, error_count, skipped_count, last_doc_num = process_jsonl_stream(
            input_path,
            output_path,
            error_path,
        )

        output_lines = output_path.read_text(encoding='utf-8').splitlines()
        assert success_count == 2
        assert error_count == 0
        assert skipped_count == 0
        assert last_doc_num == 2
        assert len(output_lines) == 2
        assert "stale-output" not in output_lines[0]
        assert error_path.read_text(encoding='utf-8') == ""

    def test_process_jsonl_stream_appends_when_resuming(self, tmp_path):
        input_path = tmp_path / "input.jsonl"
        output_path = tmp_path / "output.jsonl"
        error_path = tmp_path / "errors.jsonl"

        records = [
            make_raw_case("甲案民事判决书", "doc-1", "（2024）京01民初1号"),
            make_raw_case("乙案民事判决书", "doc-2", "（2024）京01民初2号"),
        ]
        write_jsonl(input_path, records)

        first_result, first_error = convert_document(records[0])
        assert first_error is None
        output_path.write_text(json.dumps(first_result.__dict__, ensure_ascii=False) + "\n", encoding='utf-8')

        success_count, error_count, skipped_count, last_doc_num = process_jsonl_stream(
            input_path,
            output_path,
            error_path,
            start_from=1,
            append_output=True,
        )

        output_rows = [json.loads(line) for line in output_path.read_text(encoding='utf-8').splitlines()]
        assert success_count == 1
        assert error_count == 0
        assert skipped_count == 0
        assert last_doc_num == 2
        assert [row["wenshu_id"] for row in output_rows] == ["doc-1", "doc-2"]

    def test_main_ignores_checkpoint_without_resume(self, tmp_path, monkeypatch):
        input_path = tmp_path / "input.jsonl"
        output_path = tmp_path / "output.jsonl"
        error_path = tmp_path / "output_failed.jsonl"
        checkpoint_path = tmp_path / "output_checkpoint.json"

        write_jsonl(input_path, [make_raw_case("甲案民事判决书", "doc-1", "（2024）京01民初1号")])
        output_path.write_text("existing-output\n", encoding='utf-8')
        checkpoint_path.write_text(json.dumps({
            "input": str(input_path.absolute()),
            "output": str(output_path.absolute()),
            "errors": str(error_path.absolute()),
            "filter": "*判决书",
            "last_doc_num": 7,
            "total_success": 5,
            "total_errors": 1,
            "original": None,
            "txt": None,
            "last_run": "2026-04-30T00:00:00",
        }, ensure_ascii=False), encoding='utf-8')

        seen = {}

        def fake_process_jsonl_stream(
            input_arg,
            output_arg,
            error_arg,
            doc_filter,
            start_from=0,
            max_success=None,
            original_path=None,
            append_output=False,
        ):
            seen["input"] = input_arg
            seen["output"] = output_arg
            seen["error"] = error_arg
            seen["start_from"] = start_from
            seen["append_output"] = append_output
            seen["has_filter"] = doc_filter is not None
            return 0, 0, 0, start_from

        monkeypatch.setattr(convert_cli, "process_jsonl_stream", fake_process_jsonl_stream)
        monkeypatch.setattr(sys, "argv", [
            "python",
            str(input_path),
            "--output",
            str(output_path),
        ])

        convert_cli.main()

        assert seen["input"] == input_path
        assert seen["output"] == output_path
        assert seen["error"] == error_path
        assert seen["start_from"] == 0
        assert seen["append_output"] is False
        assert seen["has_filter"] is False

    def test_main_resume_auto_resolves_matching_checkpoint(self, tmp_path, monkeypatch):
        input_path = tmp_path / "input.jsonl"
        output_path = tmp_path / "output.jsonl"
        error_path = tmp_path / "output_failed.jsonl"
        checkpoint_path = tmp_path / "output_checkpoint.json"

        write_jsonl(input_path, [make_raw_case("甲案民事判决书", "doc-1", "（2024）京01民初1号")])
        output_path.write_text("existing-output\n", encoding='utf-8')
        checkpoint_path.write_text(json.dumps({
            "input": str(input_path.absolute()),
            "output": str(output_path.absolute()),
            "errors": str(error_path.absolute()),
            "filter": "*判决书",
            "last_doc_num": 7,
            "total_success": 5,
            "total_errors": 1,
            "original": None,
            "txt": None,
            "last_run": "2026-04-30T00:00:00",
        }, ensure_ascii=False), encoding='utf-8')

        seen = {}

        def fake_process_jsonl_stream(
            input_arg,
            output_arg,
            error_arg,
            doc_filter,
            start_from=0,
            max_success=None,
            original_path=None,
            append_output=False,
        ):
            seen["input"] = input_arg
            seen["output"] = output_arg
            seen["error"] = error_arg
            seen["start_from"] = start_from
            seen["append_output"] = append_output
            seen["matches_filter"] = doc_filter({"s1": "甲案民事判决书"})
            return 0, 0, 0, start_from

        monkeypatch.setattr(convert_cli, "process_jsonl_stream", fake_process_jsonl_stream)
        monkeypatch.setattr(sys, "argv", [
            "python",
            str(input_path),
            "--output",
            str(output_path),
            "--filter",
            "*判决书",
            "--resume",
        ])

        convert_cli.main()

        assert seen["input"] == input_path
        assert seen["output"] == output_path
        assert seen["error"] == error_path
        assert seen["start_from"] == 7
        assert seen["append_output"] is True
        assert seen["matches_filter"] is True

    def test_main_resume_explicit_checkpoint_path(self, tmp_path, monkeypatch):
        input_path = tmp_path / "input.jsonl"
        output_path = tmp_path / "output.jsonl"
        error_path = tmp_path / "output_failed.jsonl"
        checkpoint_path = tmp_path / "progress.json"

        output_path.write_text("existing-output\n", encoding='utf-8')
        checkpoint_path.write_text(json.dumps({
            "input": str(input_path.absolute()),
            "output": str(output_path.absolute()),
            "errors": str(error_path.absolute()),
            "filter": "*判决书",
            "last_doc_num": 4,
            "total_success": 3,
            "total_errors": 1,
            "original": None,
            "txt": None,
            "last_run": "2026-04-30T00:00:00",
        }, ensure_ascii=False), encoding='utf-8')

        seen = {}

        def fake_process_jsonl_stream(
            input_arg,
            output_arg,
            error_arg,
            doc_filter,
            start_from=0,
            max_success=None,
            original_path=None,
            append_output=False,
        ):
            seen["input"] = input_arg
            seen["output"] = output_arg
            seen["error"] = error_arg
            seen["start_from"] = start_from
            seen["append_output"] = append_output
            return 0, 0, 0, start_from

        monkeypatch.setattr(convert_cli, "process_jsonl_stream", fake_process_jsonl_stream)
        monkeypatch.setattr(sys, "argv", [
            "python",
            "--resume",
            str(checkpoint_path),
        ])

        convert_cli.main()

        assert seen["input"] == input_path
        assert seen["output"] == output_path
        assert seen["error"] == error_path
        assert seen["start_from"] == 4
        assert seen["append_output"] is True

    def test_main_saves_interrupt_progress_to_checkpoint(self, tmp_path, monkeypatch):
        input_path = tmp_path / "input.jsonl"
        output_path = tmp_path / "output.jsonl"
        checkpoint_path = tmp_path / "progress.json"

        write_jsonl(input_path, [make_raw_case("甲案民事判决书", "doc-1", "（2024）京01民初1号")])

        def fake_process_jsonl_stream(*args, **kwargs):
            raise ConversionInterrupted(3, 1, 2, 11)

        monkeypatch.setattr(convert_cli, "process_jsonl_stream", fake_process_jsonl_stream)
        monkeypatch.setattr(sys, "argv", [
            "python",
            str(input_path),
            "--output",
            str(output_path),
            "--checkpoint",
            str(checkpoint_path),
        ])

        with pytest.raises(SystemExit) as excinfo:
            convert_cli.main()

        checkpoint = json.loads(checkpoint_path.read_text(encoding='utf-8'))
        assert excinfo.value.code == 130
        assert checkpoint["last_doc_num"] == 11
        assert checkpoint["total_success"] == 3
        assert checkpoint["total_errors"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
