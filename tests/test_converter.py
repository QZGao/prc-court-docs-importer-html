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

from convert.html_normalizer import (
    normalize_html,
    clean_text,
    get_text_align,
    has_text_indent,
    is_date_text,
    is_signature_text,
    extract_date_components,
    remove_cjk_spaces,
    BlockType,
)
from convert.wikitext_renderer import (
    render_wikitext,
    convert_html_to_wikitext,
    format_job_title,
    format_name,
    format_signature,
    parse_signature_line,
    EN_QUAD,
    THREE_PER_EM,
)
from convert.location import (
    infer_location_from_court,
    extract_province_from_court,
)
from convert.converter import (
    convert_document,
    extract_doc_type_from_s22,
    infer_court_with_province,
    iter_json_objects,
    process_jsonl_stream,
)


# Load test cases
TEST_CASES_PATH = Path(__file__).parent / "test_cases.json"


def load_test_cases():
    """Load test cases using the converter's JSON reader."""
    with open(TEST_CASES_PATH, 'r', encoding='utf-8') as f:
        return list(iter_json_objects(f))


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


class TestSignatureDetection:
    """Tests for signature text detection."""
    
    def test_judge_signature(self):
        assert is_signature_text("审判员　　章辉")
        assert is_signature_text("审 判 员　：杨清")
        assert is_signature_text("审判长　阿勒木江吾米尔江")
    
    def test_clerk_signature(self):
        assert is_signature_text("书记员　　魏凯")
        assert is_signature_text("书 记 员　曹　文")
    
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


class TestCourtInference:
    """Tests for court name inference with province."""
    
    def test_from_s22(self):
        s22 = "江西省南昌市红谷滩区人民法院\n执行裁定书\n（2022）赣0113执6828号之一"
        result = infer_court_with_province("南昌市红谷滩区人民法院", s22)
        assert result == "江西省南昌市红谷滩区人民法院"
    
    def test_fallback_to_s2(self):
        result = infer_court_with_province("南昌市红谷滩区人民法院", "")
        assert result == "南昌市红谷滩区人民法院"


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
        """Generated wikitext should contain {{header}} template."""
        for case in test_cases:
            result, error = convert_document(case)
            if result:
                assert "{{header" in result.wikitext
    
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
        assert "|noauthor = " in wikitext
        assert "|type = 中华人民共和国执行裁定书" in wikitext
    
    def test_larger_template_usage(self):
        """Test that {{larger}} template is used for title."""
        html = """
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>北京市高级人民法院</div>
        <div style='TEXT-ALIGN: center; FONT-SIZE: 18pt;'>民事判决书</div>
        <div style='TEXT-ALIGN: right;'>（2024）京民终1号</div>
        """
        wikitext = convert_html_to_wikitext(html, "标题")
        
        assert "{{larger|北京市高级人民法院}}" in wikitext
        assert "{{larger|民事判决书}}" in wikitext


class TestSignatureFormatting:
    """Tests for signature formatting with proper spacing."""
    
    def test_parse_signature_line(self):
        """Test parsing signature lines into job and name."""
        job, name = parse_signature_line("审判员　　章辉")
        assert job == "审判员"
        assert name == "章辉"
        
        job, name = parse_signature_line("审 判 员　：杨清")
        assert job == "审判员"
        assert name == "杨清"
        
        job, name = parse_signature_line("人民陪审员　刘瑞璐")
        assert job == "人民陪审员"
        assert name == "刘瑞璐"
    
    def test_format_job_title_3char(self):
        """Test 3-char job titles are separated by En Quad."""
        result = format_job_title("审判长")
        assert result == f"审{EN_QUAD}判{EN_QUAD}长"
        
        result = format_job_title("审判员")
        assert result == f"审{EN_QUAD}判{EN_QUAD}员"
        
        result = format_job_title("书记员")
        assert result == f"书{EN_QUAD}记{EN_QUAD}员"
    
    def test_format_job_title_4char(self):
        """Test 4-char job titles are separated by Three-Per-Em Space."""
        result = format_job_title("法官助理")
        assert result == f"法{THREE_PER_EM}官{THREE_PER_EM}助{THREE_PER_EM}理"
    
    def test_format_job_title_5char(self):
        """Test 5-char job titles remain unchanged."""
        result = format_job_title("人民陪审员")
        assert result == "人民陪审员"
        
        result = format_job_title("校对责任人")
        assert result == "校对责任人"
    
    def test_format_job_title_daili(self):
        """Test 代理 prefix formatting."""
        result = format_job_title("代理审判员")
        # 代理 + 审判员: 代理审判员
        assert result == "代理审判员"
    
    def test_format_name_2char(self):
        """Test 2-char names have En Quad in the middle."""
        result = format_name("王杨")
        assert result == f"王{EN_QUAD}杨"
        
        result = format_name("石君")
        assert result == f"石{EN_QUAD}君"
    
    def test_format_name_3char(self):
        """Test 3-char names remain unchanged."""
        result = format_name("周海龙")
        assert result == "周海龙"
    
    def test_format_name_4plus_char(self):
        """Test 4+ char names remain unchanged."""
        result = format_name("哈里木拉提")
        assert result == "哈里木拉提"
    
    def test_format_signature_complete(self):
        """Test complete signature formatting."""
        result = format_signature("审判长　　谢朝彪")
        # Should be: 审　判　长　　谢朝彪
        assert f"审{EN_QUAD}判{EN_QUAD}长" in result
        assert f"{EN_QUAD}{EN_QUAD}谢朝彪" in result
        
        result = format_signature("法官助理　　王杨")
        # Should be: 法 官 助 理　　王　杨
        assert f"法{THREE_PER_EM}官{THREE_PER_EM}助{THREE_PER_EM}理" in result
        assert f"王{EN_QUAD}杨" in result


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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
