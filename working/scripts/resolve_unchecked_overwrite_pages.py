#!/usr/bin/env python
"""
Resolve [[Category:覆盖版本未检查的裁判文书]] pages after manual review.

The script is intentionally conservative: page classifications are curated from
the revision audit for the 94 pages in the category on 2026-06-02.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

import pywikibot

from convert.html_normalizer import is_date_text, strip_signature_leading_junk
from convert.wikitext_renderer import parse_signature_entries
from upload.conflict_resolution import (
    build_grouped_versions_page_content,
    build_versions_page_content,
)
from upload.mediawiki import can_move_over_redirect, configure_throttle, get_site
from upload.page_metadata import (
    build_case_title_from_metadata,
    normalize_case_number,
    normalize_court_name,
    normalize_doc_type,
    parse_header_metadata,
    parse_template_metadata,
)
from upload.uploader import UNCHECKED_OVERWRITE_CATEGORY_RE


IMPORT_SUMMARY_PREFIX = "Imported from 裁判文书网"
EDIT_SUMMARY = "清理覆盖版本检查"
MOVE_SUMMARY = "移动至具体案号页面，原标题改为消歧义页"
CATEGORY_TITLE = "Category:覆盖版本未检查的裁判文书"

HEADER_PREFIXES = ("{{header/裁判文书",)
LEGACY_HEADER_PREFIXES = ("{{header",)
HEADER_START_RE = re.compile(r"^\s*\{\{\s*header(?:/裁判文书)?\b", re.I)
HEADER_PARAM_RE = re.compile(r"^\s*\|\s*([^=\n]+?)\s*=\s*(.*?)\s*$")
CASE_NUMBER_RE = re.compile(
    r"[（(]20\d{2}[）)][^，。；：:\s\n]{1,35}?号(?:之[一二三四五六七八九十百千万〇零○]+)?"
)
COURT_RE = re.compile(r"([\u4e00-\u9fff（）()·]{2,60}?(?:人民法院|运输法院))")
CN_DATE_RE = re.compile(
    r"([二〇零○一二三四五六七八九]{4})年([一二三四五六七八九十]{1,3})月([一二三四五六七八九十]{1,3})日"
)
AR_DATE_RE = re.compile(r"(20\d{2})年(\d{1,2})月(\d{1,2})日")
SIGNATURE_START_RE = re.compile(r"^\s*\{\{\s*裁判文书署名\s*\|\s*1\s*=\s*$")
OLD_SIGNATURE_START_RE = re.compile(r"^\s*\{\{\s*署名\s*\|\s*$")
PRC_EXEMPT_RE = re.compile(r"^\s*\{\{\s*PD-PRC-exempt\s*\}\}\s*$", re.I)
SIGNATURE_ROLES = ("人民陪审员", "法官助理", "代书记员", "审判长", "审判员", "书记员")
INLINE_SEGMENT_PATTERNS = [
    r"原告[:：]",
    r"被告[:：]",
    r"第三人[:：]",
    r"上诉人(?:（[^）]+）)?[:：]",
    r"被上诉人(?:（[^）]+）)?[:：]",
    r"委托诉讼代理人[:：]",
    r"(?<=[。；])原告[^。]{0,80}?向本院提出诉讼请求",
    r"(?<=[。；])原告[^。]{0,80}?诉称",
    r"(?<=[。；])被告[^。]{0,80}?辩称",
    r"(?<=[。；])被告[^。]{0,80}?未作答辩",
    r"经审理查明",
    r"另查明",
    r"本院认为",
    r"判决如下[:：]",
    r"如果未按本判决指定",
    r"案件受理费",
    r"本判决为终审判决",
    r"如不服本判决",
]

DOC_TYPES = [
    "刑事判决书",
    "行政判决书",
    "民事判决书",
    "刑事裁定书",
    "行政裁定书",
    "民事裁定书",
    "民事调解书",
    "支付令",
    "民事决定书",
]

# Pages where the reverted import is the same document and should be absorbed
# into the current page with docid2 and minor title/date notes where useful.
MERGE_TITLES = {
    "上海市嘉定区人民法院（2023）沪0114民初13728号民事判决书",
    "上海市嘉定区人民法院（2023）沪0114民初26039号民事判决书",
    "上海市嘉定区人民法院（2023）沪0114民初26066号民事判决书",
    "上海市奉贤区人民法院（2023）沪0120民初20559号民事判决书",
    "上海市奉贤区人民法院（2023）沪0120民初8374号民事判决书",
    "上海市宝山区人民法院（2023）沪0113民初39211号民事判决书",
    "上海市徐汇区人民法院（2023）沪0104民初17032号民事判决书",
    "上海市徐汇区人民法院（2023）沪0104民初23100号民事判决书",
    "上海市徐汇区人民法院（2023）沪0104民初23759号民事判决书",
    "上海市徐汇区人民法院（2023）沪0104民初25331号民事判决书",
    "上海市徐汇区人民法院（2023）沪0104民初25333号民事判决书",
    "上海市徐汇区人民法院（2023）沪0104民初26467号民事判决书",
    "上海市徐汇区人民法院（2023）沪0104民初26727号民事判决书",
    "上海市徐汇区人民法院（2023）沪0104民初29960号民事判决书",
    "上海市徐汇区人民法院（2023）沪0104民初32490号民事判决书",
    "上海市徐汇区人民法院（2023）沪0104民初32507号民事判决书",
    "上海市徐汇区人民法院（2023）沪0104民初866号民事判决书",
    "上海市松江区人民法院（2023）沪0117民初17260号民事判决书",
    "上海市松江区人民法院（2023）沪0117民初19464号民事判决书",
    "上海市松江区人民法院（2023）沪0117民初19670号民事判决书",
    "上海市松江区人民法院（2023）沪0117民初20037号民事判决书",
    "上海市松江区人民法院（2023）沪0117民初20911号民事判决书",
    "上海市松江区人民法院（2023）沪0117民初21811号民事判决书",
    "上海市松江区人民法院（2023）沪0117民初22343号民事判决书",
    "上海市松江区人民法院（2023）沪0117民初24011号民事判决书",
    "上海市松江区人民法院（2023）沪0117民初24666号民事判决书",
    "上海市松江区人民法院（2023）沪0117民初26236号民事判决书",
    "上海市松江区人民法院（2024）沪0117民初6766号民事判决书",
    "上海市松江区人民法院（2024）沪0117民初946号民事判决书",
    "上海市浦东新区人民法院（2023）沪0115民初116584号民事判决书",
    "上海市浦东新区人民法院（2023）沪0115民初7520号民事判决书",
    "上海市浦东新区人民法院（2023）沪0115民初77927号民事判决书",
    "上海市浦东新区人民法院（2024）沪0115民初17851号民事判决书",
    "上海市浦东新区人民法院（2024）沪0115民初8297号民事判决书",
    "上海市长宁区人民法院（2023）沪0105民初37264号民事判决书",
    "上海市长宁区人民法院（2023）沪0105民初37273号民事判决书",
    "上海市闵行区人民法院（2023）沪0112民初43827号民事判决书",
    "上海市静安区人民法院（2023）沪0106民初25972号民事判决书",
    "上海市静安区人民法院（2023）沪0106民初35314号民事判决书",
    "上海市静安区人民法院（2023）沪0106民初35317号民事判决书",
    "上海市静安区人民法院（2023）沪0106民初46530号民事判决书",
    "上海市静安区人民法院（2023）沪0106民初7201号民事判决书",
    "上海市静安区人民法院（2024）沪0106民初382号民事判决书",
    "上海市静安区人民法院（2024）沪0106民初389号民事判决书",
    "上海铁路运输法院（2022）沪7101民初1507号民事判决书",
    "上海铁路运输法院（2023）沪7101民初1556号民事判决书",
    "乐昌市湘府家宴店、远晟文化传承（韶关）有限公司餐饮服务合同纠纷民事一审民事判决书",
    "吴海君、徐琴琴民间借贷纠纷一审民事判决书",
    "徐某某、徐某某等机动车交通事故责任纠纷民事一审民事判决书",
    "支某、某公司装饰装修合同纠纷二审民事判决书",
    "某金融资产管理有限公司、艾某力金融不良债权追偿纠纷民事一审民事判决书",
    "毛小宽、浙江恒晨印染有限公司劳动争议一审民事判决书",
    "河南自由贸易试验区郑州片区人民法院（2024）豫0194刑初110号刑事判决书",
    "河南自由贸易试验区郑州片区人民法院（2024）豫0194刑初304号刑事判决书",
    "韶关得利包装科技有限公司、广东宝创环保新材料制品有限公司买卖合同纠纷民事一审民事判决书",
}

# Same case number, but genuinely different documents with different canonical
# title fields. These are stored at canonical titles; the case-number title
# becomes the disambiguation page.
SAME_CASE_SPLIT_TITLES = {
    "何文龙危险驾驶罪刑事一审刑事判决书",
    "叶丹毅、何继波承揽合同纠纷一审民事判决书",
    "宁夏回族自治区西吉县人民法院（2024）宁0422刑初35号刑事判决书",
    "廖晓飞、杨茂德民间借贷纠纷一审民事判决书",
    "杭州吉喆健康管理有限公司、张龄文等买卖合同纠纷二审民事判决书",
    "浙商财产保险股份有限公司安吉支公司、王凯等保险人代位求偿权纠纷一审民事判决书",
    "王恩稳、张磊机动车交通事故责任纠纷一审民事判决书",
    "王科勇、任铭民间借贷纠纷一审民事判决书",
}

# The rest of the reviewed pages are true title collisions and are split into
# case-number pages plus a disambiguation page at the original title.
NORMAL_SPLIT_TITLES = {
    "于某某、金某某民间借贷纠纷民事一审民事判决书",
    "代某某危险驾驶罪、危险驾驶罪刑事一审刑事判决书",
    "任某某、宋某某民事一审民事判决书",
    "何某1、何某2等机动车交通事故责任纠纷民事一审民事判决书",
    "侵害实用新型专利权纠纷一审民事判决书",
    "刘某与李某劳务合同纠纷一审民事判决书",
    "刘某某、季某某民间借贷纠纷民事一审民事判决书",
    "刘某金危险驾驶罪、危险驾驶罪刑事一审刑事判决书",
    "包头市某某物业服务有限责任公司、王某民事一审民事判决书",
    "包某林与杨某琼,任某飞民间借贷纠纷一审民事判决书",
    "卢某某民事一审民事判决书",
    "台州某某公司、李某某等申请支付令督促程序(支付令)民事令",
    "台州某某公司、王某某等申请支付令督促程序(支付令)民事令",
    "夏某、白某等机动车交通事故责任纠纷民事一审民事判决书",
    "张某、李某离婚纠纷民事一审民事判决书",
    "张某与田某买卖合同纠纷一审民事判决书",
    "张某某一审行政判决书",
    "徐某某、吴某某民间借贷纠纷民事一审民事判决书",
    "方某、公司A金融借款合同纠纷一审民事判决书",
    "李×、刘×劳务合同纠纷民事一审民事判决书",
    "某公司甲与某公司乙建筑设备租赁合同纠纷一审民事判决书",
    "某某商业银行股份有限公司、陈某某申请支付令督促程序(支付令)民事令",
    "某银行股份有限公司重庆四公里支行与黄某信用卡纠纷一审民事判决书",
    "某银行股份有限公司重庆市分行与黄某某金融借款合同纠纷一审民事判决书",
    "沈阳某物业服务有限公司、金某物业服务合同纠纷民事一审民事判决书",
    "沈阳某物业管理有限公司、张某等物业服务合同纠纷民事一审民事判决书",
    "浙江磐安农村商业银行股份有限公司、羊元德等申请支付令督促程序(支付令)民事令",
    "王某、韩某民间借贷纠纷民事一审民事判决书",
    "王某某、任某某民间借贷纠纷民事一审民事判决书",
    "秦某、郭某民事一审民事判决书",
    "陈某某、高某某民间借贷纠纷民事一审民事判决书",
}

# Current article has no recoverable concrete case number in the visible source
# text, so these are intentionally left in the maintenance category for manual
# handling rather than inventing a case-number title.
SKIP_REDACTED_CASE_TITLES = {
}

PARTIAL_EMPTY_CURRENT_SAME_CASE_TITLES = {
}

# Known current-page metadata that cannot be inferred safely from headers alone.
CURRENT_METADATA_OVERRIDES = {
    "于某某、金某某民间借贷纠纷民事一审民事判决书": {
        "court": "内蒙古自治区开鲁县人民法院",
        "type": "民事判决书",
        "案号": "（2024）内0523民初1661号",
        "year": "2024",
        "month": "5",
        "day": "13",
        "loc": "内蒙古自治区开鲁县",
    },
    "代某某危险驾驶罪、危险驾驶罪刑事一审刑事判决书": {
        "court": "贵州省遵义市汇川区人民法院",
        "type": "刑事判决书",
        "案号": "（2021）黔0303刑初307号",
        "year": "2021",
        "month": "9",
        "day": "7",
        "loc": "遵义市",
    },
    "何某1、何某2等机动车交通事故责任纠纷民事一审民事判决书": {
        "court": "广东省广州市从化区人民法院",
        "type": "民事判决书",
        "案号": "（2023）粤0117民初145号",
        "year": "2023",
        "month": "4",
        "day": "6",
        "loc": "广东省广州市",
    },
    "侵害实用新型专利权纠纷一审民事判决书": {
        "court": "浙江省杭州市中级人民法院",
        "type": "民事判决书",
        "案号": "（2023）浙01民初2447号",
        "year": "2024",
        "month": "5",
        "day": "8",
        "loc": "浙江省杭州市",
    },
    "刘某与李某劳务合同纠纷一审民事判决书": {
        "court": "青海省治多县人民法院",
        "type": "民事判决书",
        "案号": "（2021）青2724民初51号",
        "year": "2021",
        "month": "5",
        "day": "20",
        "loc": "青海省治多县",
    },
    "刘某某、季某某民间借贷纠纷民事一审民事判决书": {
        "court": "江苏省常州市武进区人民法院",
        "type": "民事判决书",
        "案号": "（2xxx）苏0xxx民初5xxx号",
        "year": "",
        "month": "",
        "day": "",
        "loc": "江苏省常州市",
    },
    "刘某金危险驾驶罪、危险驾驶罪刑事一审刑事判决书": {
        "court": "广东省龙门县人民法院",
        "type": "刑事判决书",
        "案号": "（2024）粤1324刑初165号",
        "year": "2024",
        "month": "6",
        "day": "26",
        "loc": "广东省龙门县",
    },
    "包某林与杨某琼,任某飞民间借贷纠纷一审民事判决书": {
        "court": "重庆市长寿区人民法院",
        "type": "民事判决书",
        "案号": "（2024）渝0115民初2724号",
        "year": "2024",
        "month": "7",
        "day": "11",
        "loc": "重庆市",
    },
    "卢某某民事一审民事判决书": {
        "court": "江苏省滨海县人民法院",
        "type": "民事判决书",
        "案号": "（2024）苏0922民初3370号",
        "year": "2024",
        "month": "6",
        "day": "13",
        "loc": "江苏省滨海县",
    },
    "夏某、白某等机动车交通事故责任纠纷民事一审民事判决书": {
        "court": "长春汽车经济技术开发区人民法院",
        "type": "民事判决书",
        "案号": "（2024）吉0192民初2528号",
        "year": "2024",
        "month": "7",
        "day": "29",
        "loc": "吉林省长春市",
    },
    "张某、李某离婚纠纷民事一审民事判决书": {
        "court": "大连市普兰店区人民法院",
        "type": "民事判决书",
        "案号": "（2021）辽0214民初6887号",
        "year": "2021",
        "month": "9",
        "day": "18",
        "loc": "辽宁省大连市",
    },
    "张某与田某买卖合同纠纷一审民事判决书": {
        "court": "重庆市巫溪县人民法院",
        "type": "民事判决书",
        "案号": "（2024）渝0238民初2005号",
        "year": "2024",
        "month": "9",
        "day": "29",
        "loc": "重庆市",
    },
    "张某某一审行政判决书": {
        "court": "西安铁路运输法院",
        "type": "行政判决书",
        "案号": "（2023）陕7102行初3091号",
        "year": "2023",
        "month": "12",
        "day": "26",
        "loc": "陕西省西安市",
    },
    "徐某某、吴某某民间借贷纠纷民事一审民事判决书": {
        "court": "江苏省建湖县人民法院",
        "type": "民事判决书",
        "案号": "（2024）苏0925民初3610号",
        "year": "2024",
        "month": "7",
        "day": "12",
        "loc": "江苏省建湖县",
    },
    "方某、公司A金融借款合同纠纷一审民事判决书": {
        "court": "浙江省杭州市余杭区人民法院",
        "type": "民事判决书",
        "案号": "（2024）浙0110民初5534号",
        "year": "2024",
        "month": "7",
        "day": "2",
        "loc": "浙江省杭州市",
    },
    "李×、刘×劳务合同纠纷民事一审民事判决书": {
        "court": "辽宁省台安县人民法院",
        "type": "民事判决书",
        "案号": "（2024）辽0321民初2050号",
        "year": "2024",
        "month": "6",
        "day": "13",
        "loc": "辽宁省台安县",
    },
    "某公司甲与某公司乙建筑设备租赁合同纠纷一审民事判决书": {
        "court": "重庆市云阳县人民法院",
        "type": "民事判决书",
        "案号": "（2023）渝0235民初6559号",
        "year": "2023",
        "month": "11",
        "day": "28",
        "loc": "重庆市",
    },
    "某银行股份有限公司重庆市分行与黄某某金融借款合同纠纷一审民事判决书": {
        "court": "重庆市渝中区人民法院",
        "type": "民事判决书",
        "案号": "（2023）渝0103民初25161号",
        "year": "2023",
        "month": "8",
        "day": "29",
        "loc": "重庆市",
    },
    "沈阳某物业服务有限公司、金某物业服务合同纠纷民事一审民事判决书": {
        "court": "辽宁省沈阳市皇姑区人民法院",
        "type": "民事判决书",
        "案号": "（2024）辽0105民初4153号",
        "year": "2024",
        "month": "4",
        "day": "19",
        "loc": "辽宁省沈阳市",
    },
    "沈阳某物业管理有限公司、张某等物业服务合同纠纷民事一审民事判决书": {
        "court": "辽宁省沈阳市皇姑区人民法院",
        "type": "民事判决书",
        "案号": "（2024）辽0105民初354号",
        "year": "2024",
        "month": "2",
        "day": "26",
        "loc": "辽宁省沈阳市",
    },
    "王某、韩某民间借贷纠纷民事一审民事判决书": {
        "court": "甘肃省漳县人民法院",
        "type": "民事判决书",
        "案号": "（2024）甘1125民初224号",
        "year": "2024",
        "month": "3",
        "day": "22",
        "loc": "甘肃省漳县",
    },
    "王某某、任某某民间借贷纠纷民事一审民事判决书": {
        "court": "四川省富顺县人民法院",
        "type": "民事判决书",
        "案号": "（2023）川0322民初3407号",
        "year": "2023",
        "month": "11",
        "day": "23",
        "loc": "四川省富顺县",
    },
    "秦某、郭某民事一审民事判决书": {
        "court": "鄂托克旗人民法院",
        "type": "民事判决书",
        "案号": "（2024）内0624民初2900号",
        "year": "2024",
        "month": "9",
        "day": "27",
        "loc": "内蒙古自治区鄂托克旗",
    },
    "陈某某、高某某民间借贷纠纷民事一审民事判决书": {
        "court": "江苏省常州市武进区人民法院",
        "type": "民事判决书",
        "案号": "（2xxx）苏0xxx民初3xxx号",
        "year": "",
        "month": "",
        "day": "",
        "loc": "江苏省常州市",
    },
}


@dataclass
class RevisionData:
    revid: int
    parentid: int
    comment: str
    text: str
    metadata: dict[str, str]


@dataclass
class PreparedPage:
    title: str
    text: str
    metadata: dict[str, str]
    case_title: str


class Resolver:
    def __init__(self, *, dry_run: bool, interval: float, maxlag: int, only: set[str] | None = None):
        self.site = get_site()
        self.dry_run = dry_run
        self.interval = interval
        self.maxlag = maxlag
        self.only = only
        self.last_write_time = 0.0
        self.report: list[dict[str, object]] = []

    def wait(self) -> None:
        if self.dry_run:
            return
        elapsed = time.monotonic() - self.last_write_time
        if self.last_write_time and elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self.last_write_time = time.monotonic()

    def save(self, title: str, text: str, summary: str = EDIT_SUMMARY) -> None:
        if self.dry_run:
            print(f"    DRY save [[{title}]] len={len(text)}")
            return
        self.wait()
        page = pywikibot.Page(self.site, title)
        page.text = text
        page.save(summary=summary, minor=False, botflag=True)

    def create(self, title: str, text: str, summary: str = EDIT_SUMMARY) -> None:
        page = pywikibot.Page(self.site, title)
        if page.exists():
            raise RuntimeError(f"destination already exists: [[{title}]]")
        if self.dry_run:
            print(f"    DRY create [[{title}]] len={len(text)}")
            return
        self.wait()
        page.text = text
        page.save(summary=summary, minor=False, botflag=True)

    def move(self, source: str, target: str) -> None:
        target_page = pywikibot.Page(self.site, target)
        if target_page.exists() and not can_move_over_redirect(source, target):
            raise RuntimeError(f"move destination already exists: [[{target}]]")
        if self.dry_run:
            print(f"    DRY move [[{source}]] -> [[{target}]]")
            return
        self.wait()
        page = pywikibot.Page(self.site, source)
        if target_page.exists() and can_move_over_redirect(source, target):
            request = self.site.simple_request(
                action="move",
                format="json",
                formatversion="2",
                to=target,
                reason=MOVE_SUMMARY,
                token=self.site.tokens["csrf"],
                ignorewarnings="1",
                maxlag=self.maxlag,
            )
            request["from"] = source
            payload = request.submit()
            if "error" in payload:
                raise RuntimeError(payload["error"])
            return
        page.move(target, reason=MOVE_SUMMARY, noredirect=False)

    def run(self) -> None:
        titles = self.fetch_category_titles()
        configured = MERGE_TITLES | NORMAL_SPLIT_TITLES | SAME_CASE_SPLIT_TITLES
        missing = [title for title in titles if title not in configured]
        if missing:
            raise RuntimeError(f"unclassified pages: {missing}")

        for index, title in enumerate(titles, start=1):
            if self.only and title not in self.only:
                continue
            print(f"\n[{index}/{len(titles)}] [[{title}]]")
            try:
                if title in SKIP_REDACTED_CASE_TITLES:
                    print("    skip: current page has no recoverable concrete case number")
                    action = "skip_redacted_case_number"
                elif title in PARTIAL_EMPTY_CURRENT_SAME_CASE_TITLES:
                    self.resolve_partial_empty_current_same_case(title)
                    action = "partial_empty_current_same_case"
                elif title in MERGE_TITLES:
                    self.resolve_merge(title)
                    action = "merge"
                elif title in SAME_CASE_SPLIT_TITLES:
                    self.resolve_same_case_split(title)
                    action = "same_case_split"
                else:
                    self.resolve_normal_split(title)
                    action = "normal_split"
                self.report.append({"title": title, "action": action, "status": "ok"})
            except Exception as exc:
                self.report.append({"title": title, "status": "failed", "error": repr(exc)})
                print(f"    FAILED: {exc}")
                raise

    def fetch_category_titles(self) -> list[str]:
        members: list[str] = []
        continuation: dict[str, object] = {}
        while True:
            payload = self.site.simple_request(
                action="query",
                format="json",
                formatversion="2",
                list="categorymembers",
                cmtitle=CATEGORY_TITLE,
                cmnamespace="0",
                cmlimit="500",
                maxlag=self.maxlag,
                **continuation,
            ).submit()
            members.extend(member["title"] for member in payload.get("query", {}).get("categorymembers", []))
            continuation = payload.get("continue", {})
            if not continuation:
                return members

    def fetch_revisions(self, title: str) -> list[RevisionData]:
        page = pywikibot.Page(self.site, title)
        if not page.exists():
            raise RuntimeError(f"page does not exist: [[{title}]]")
        revisions = []
        for revision in page.revisions(content=True):
            text = revision.text or ""
            revisions.append(
                RevisionData(
                    revid=int(revision.revid),
                    parentid=int(revision.parentid or 0),
                    comment=revision.comment or "",
                    text=text,
                    metadata=self.infer_metadata(text, title),
                )
            )
        return revisions

    def import_revisions(self, revisions: list[RevisionData], current_docids: set[str]) -> list[RevisionData]:
        selected: list[RevisionData] = []
        seen_docids: set[str] = set()
        for revision in revisions:
            if not revision.comment.startswith(IMPORT_SUMMARY_PREFIX):
                continue
            docid = revision.metadata.get("docid", "")
            key = docid or f"revid:{revision.revid}"
            if key in current_docids or key in seen_docids:
                continue
            seen_docids.add(key)
            selected.append(revision)
        return selected

    def resolve_merge(self, title: str) -> None:
        revisions = self.fetch_revisions(title)
        current = revisions[0]
        current_docids = {current.metadata.get("docid", ""), current.metadata.get("docid2", "")}
        imports = self.import_revisions(revisions, current_docids)
        alt_docids = [revision.metadata.get("docid", "") for revision in imports if revision.metadata.get("docid")]
        alt_titles = [clean_title_value(revision.metadata.get("title", "")) for revision in imports]

        metadata = dict(current.metadata)
        metadata.update(CURRENT_METADATA_OVERRIDES.get(title, {}))
        for docid in alt_docids:
            add_docid(metadata, docid)

        title_value = metadata.get("title") or title
        alternate_titles = [alt for alt in alt_titles if alt and alt != clean_title_value(title_value)]
        if alternate_titles:
            title_value = build_alternate_title_value(title_value, alternate_titles[0])
        metadata["title"] = title_value

        text = rebuild_page(current.text, metadata, remove_intro=True)
        text = apply_special_merge_notes(title, text, imports)
        print(f"    merge docids={alt_docids} alts={alternate_titles[:1]}")
        self.save(title, text)

    def resolve_normal_split(self, title: str) -> None:
        revisions = self.fetch_revisions(title)
        current = revisions[0]
        current_metadata = dict(current.metadata)
        current_metadata.update(CURRENT_METADATA_OVERRIDES.get(title, {}))
        current_metadata["title"] = f"[[{clean_title_value(current_metadata.get('title') or title)}]]"
        current_prepared = self.prepare_page(title, current.text, current_metadata, disambig_title=title)
        imports = self.import_revisions(revisions, {current_metadata.get("docid", ""), current_metadata.get("docid2", "")})
        if not imports:
            raise RuntimeError("normal split has no import revisions")

        import_pages = [
            self.prepare_page(
                title,
                revision.text,
                {**revision.metadata, "title": f"[[{clean_title_value(revision.metadata.get('title') or title)}]]"},
                disambig_title=title,
            )
            for revision in imports
        ]
        all_entries = [current_prepared.case_title] + [page.case_title for page in import_pages]
        print(f"    current -> [[{current_prepared.case_title}]]")
        for page in import_pages:
            print(f"    import -> [[{page.case_title}]]")

        self.move(title, current_prepared.case_title)
        self.save(current_prepared.case_title, current_prepared.text)
        for page in import_pages:
            self.create(page.case_title, page.text)

        courts = {page.metadata["court"] for page in [current_prepared, *import_pages]}
        doc_types = {page.metadata["type"] for page in [current_prepared, *import_pages]}
        if len(courts) == 1:
            disambig_text = build_versions_page_content(
                title=title,
                noauthor=next(iter(courts)),
                entry_titles=all_entries,
                header_type=next(iter(doc_types)) if len(doc_types) == 1 else current_prepared.metadata["type"],
            )
        else:
            grouped: dict[str, list[str]] = {}
            for page in [current_prepared, *import_pages]:
                grouped.setdefault(page.metadata["court"], []).append(page.case_title)
            disambig_text = build_grouped_versions_page_content(
                title=title,
                header_type=current_prepared.metadata["type"],
                court_entries=grouped,
            )
        self.save(title, disambig_text, "创建消歧义页")

    def resolve_same_case_split(self, title: str) -> None:
        revisions = self.fetch_revisions(title)
        current = revisions[0]
        current_metadata = dict(current.metadata)
        current_metadata.update(CURRENT_METADATA_OVERRIDES.get(title, {}))
        case_title = require_case_title(current_metadata, title)
        current_canonical = clean_title_value(current_metadata.get("title") or title)
        current_metadata["title"] = f"[[{case_title}|{current_canonical}]]"
        current_text = rebuild_page(current.text, current_metadata, remove_intro=True)
        imports = self.import_revisions(revisions, {current_metadata.get("docid", ""), current_metadata.get("docid2", "")})
        if not imports:
            raise RuntimeError("same-case split has no import revisions")

        import_pages: list[PreparedPage] = []
        for revision in imports:
            metadata = dict(revision.metadata)
            import_case_title = require_case_title(metadata, title)
            if import_case_title != case_title:
                raise RuntimeError(f"same-case split import has different case title: {import_case_title}")
            canonical = clean_title_value(metadata.get("title") or title)
            metadata["title"] = f"[[{case_title}|{canonical}]]"
            import_pages.append(
                PreparedPage(
                    title=canonical,
                    text=rebuild_page(revision.text, metadata, remove_intro=True),
                    metadata=metadata,
                    case_title=case_title,
                )
            )

        print(f"    case-number disambig [[{case_title}]]")
        print(f"    current canonical [[{current_canonical}]]")
        for page in import_pages:
            print(f"    import canonical [[{page.title}]]")

        if title == case_title:
            self.move(title, current_canonical)
            self.save(current_canonical, current_text)
        else:
            self.save(title, current_text)

        for page in import_pages:
            if page.title == title:
                raise RuntimeError(f"import canonical title collides with current title: [[{title}]]")
            self.create(page.title, page.text)

        entries = [current_canonical] + [page.title for page in import_pages]
        disambig_text = build_versions_page_content(
            title=case_title,
            noauthor=current_metadata["court"],
            entry_titles=entries,
            header_type=current_metadata["type"],
        )
        self.save(case_title, disambig_text, "创建消歧义页")

    def resolve_partial_empty_current_same_case(self, title: str) -> None:
        revisions = self.fetch_revisions(title)
        current = revisions[0]
        current_metadata = dict(current.metadata)
        current_metadata.update(CURRENT_METADATA_OVERRIDES.get(title, {}))
        case_title = require_case_title(current_metadata, title)
        current_canonical = clean_title_value(current_metadata.get("title") or title)
        imports = self.import_revisions(revisions, {current_metadata.get("docid", ""), current_metadata.get("docid2", "")})
        if not imports:
            raise RuntimeError("partial same-case split has no import revisions")

        import_pages: list[PreparedPage] = []
        for revision in imports:
            metadata = dict(revision.metadata)
            import_case_title = require_case_title(metadata, title)
            if import_case_title != case_title:
                raise RuntimeError(f"partial same-case split import has different case title: {import_case_title}")
            canonical = clean_title_value(metadata.get("title") or title)
            metadata["title"] = f"[[{case_title}|{canonical}]]"
            import_pages.append(
                PreparedPage(
                    title=canonical,
                    text=rebuild_page(revision.text, metadata, remove_intro=True),
                    metadata=metadata,
                    case_title=case_title,
                )
            )

        print(f"    current empty canonical left in category [[{current_canonical}]]")
        print(f"    case-number disambig [[{case_title}]]")
        for page in import_pages:
            print(f"    import canonical [[{page.title}]]")

        for page in import_pages:
            if page.title == title:
                raise RuntimeError(f"import canonical title collides with current title: [[{title}]]")
            self.create(page.title, page.text)

        entries = [current_canonical] + [page.title for page in import_pages]
        disambig_text = build_versions_page_content(
            title=case_title,
            noauthor=current_metadata["court"],
            entry_titles=entries,
            header_type=current_metadata["type"],
        )
        self.save(case_title, disambig_text, "创建消歧义页")

    def prepare_page(
        self,
        original_title: str,
        text: str,
        metadata: dict[str, str],
        *,
        disambig_title: str,
    ) -> PreparedPage:
        metadata = dict(metadata)
        metadata["title"] = metadata.get("title") or f"[[{disambig_title}]]"
        case_title = require_case_title(metadata, original_title)
        return PreparedPage(
            title=case_title,
            text=rebuild_page(text, metadata, remove_intro=True),
            metadata=metadata,
            case_title=case_title,
        )

    def infer_metadata(self, text: str, page_title: str) -> dict[str, str]:
        metadata = parse_header_metadata(text) or parse_template_metadata(text, LEGACY_HEADER_PREFIXES) or {}
        result = {
            "title": metadata.get("title", "").strip() or page_title,
            "court": normalize_court_name(metadata.get("court", "") or metadata.get("noauthor", "")),
            "type": normalize_doc_type(metadata.get("type", "")),
            "案号": normalize_case_number(metadata.get("案号", "")),
            "year": metadata.get("year", "").strip(),
            "month": metadata.get("month", "").strip(),
            "day": metadata.get("day", "").strip(),
            "loc": metadata.get("loc", "").strip(),
            "docid": metadata.get("docid", "").strip(),
            "docid2": metadata.get("docid2", "").strip(),
        }
        plain = visible_text(text)
        if not result["court"]:
            result["court"] = infer_court(plain)
        if not result["案号"]:
            result["案号"] = infer_case_number(plain)
        if not result["type"]:
            result["type"] = infer_doc_type(text, page_title)
        if not (result["year"] and result["month"] and result["day"]):
            year, month, day = infer_date(text)
            result["year"] = result["year"] or year
            result["month"] = result["month"] or month
            result["day"] = result["day"] or day
        if not result["loc"] and result["court"]:
            result["loc"] = infer_location(result["court"])
        return result


def clean_title_value(value: str) -> str:
    value = (value or "").strip()
    if value.startswith("[[") and value.endswith("]]"):
        inner = value[2:-2]
        if "|" in inner:
            return inner.split("|", 1)[1].strip()
        return inner.strip()
    if value.startswith("[[") and "|" in value and value.endswith("]]"):
        return value[2:-2].split("|", 1)[1].strip()
    return value


def build_alternate_title_value(current_value: str, alternate: str) -> str:
    current_value = current_value.strip()
    display = clean_title_value(current_value)
    alternate = clean_title_value(alternate)
    if not alternate or alternate == display:
        return current_value
    # Prefer the title with fewer redaction markers/placeholder tokens as the
    # displayed value, per the requested {{另}} semantics.
    if redaction_score(alternate) < redaction_score(display):
        primary, secondary = alternate, display
    else:
        primary, secondary = display, alternate
    shown = f"{{{{另|{primary}|{secondary}}}}}"
    if current_value.startswith("[[") and current_value.endswith("]]"):
        target = clean_title_value(current_value)
        return f"[[{target}|{shown}]]"
    return shown


def redaction_score(value: str) -> int:
    return (
        value.count("某")
        + value.count("×")
        + value.count("X")
        + value.count("x")
        + 3 * value.count("{{PRC-redact")
    )


def add_docid(metadata: dict[str, str], docid: str) -> None:
    if not docid:
        return
    if metadata.get("docid") == docid or metadata.get("docid2") == docid:
        return
    if not metadata.get("docid"):
        metadata["docid"] = docid
        return
    if not metadata.get("docid2"):
        metadata["docid2"] = docid
        return
    index = 3
    while metadata.get(f"docid{index}"):
        if metadata[f"docid{index}"] == docid:
            return
        index += 1
    metadata[f"docid{index}"] = docid


def require_case_title(metadata: dict[str, str], context: str) -> str:
    normalized = {
        "court": normalize_court_name(metadata.get("court", "")),
        "type": normalize_doc_type(metadata.get("type", "")),
        "案号": normalize_case_number(metadata.get("案号", "")),
    }
    if not all(normalized.values()):
        raise RuntimeError(f"missing case metadata for {context}: {normalized}")
    case_title = build_case_title_from_metadata(normalized)
    if not case_title:
        raise RuntimeError(f"could not build case title for {context}: {normalized}")
    return case_title


def rebuild_page(text: str, metadata: dict[str, str], *, remove_intro: bool) -> str:
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    body = strip_existing_header(lines)
    body = UNCHECKED_OVERWRITE_CATEGORY_RE.sub("", "\n".join(body))
    body_lines = body.split("\n")
    if remove_intro:
        body_lines = remove_intro_metadata(body_lines, metadata)
    body_lines = expand_inline_body_lines(body_lines, metadata)
    if remove_intro:
        body_lines = remove_intro_metadata(body_lines, metadata)
    body_lines = normalize_standalone_signature_clusters(body_lines)
    body_lines = normalize_signature_blocks(body_lines)
    body_lines = remove_empty_signature_blocks(body_lines)
    body_lines = gap_body_lines(body_lines)
    header = build_header(metadata)
    result = header + "\n" + "\n".join(body_lines).strip() + "\n"
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result


def strip_existing_header(lines: list[str]) -> list[str]:
    for start, line in enumerate(lines):
        if HEADER_START_RE.match(line):
            for end in range(start + 1, len(lines)):
                if lines[end].strip() == "}}":
                    return lines[:start] + lines[end + 1 :]
            raise RuntimeError("unterminated header")
    return lines


def build_header(metadata: dict[str, str]) -> str:
    keys = ["title", "court", "type", "案号", "year", "month", "day", "loc", "docid", "docid2"]
    extra_docids = sorted(
        [key for key in metadata if re.fullmatch(r"docid[3-9]\d*", key)],
        key=lambda key: int(key.removeprefix("docid")),
    )
    keys.extend(extra_docids)
    lines = ["{{Header/裁判文书"]
    for key in keys:
        value = metadata.get(key, "")
        if key.startswith("docid") and not value:
            continue
        lines.append(f"|{key} = {value}")
    lines.append("}}")
    return "\n".join(lines)


def remove_intro_metadata(lines: list[str], metadata: dict[str, str]) -> list[str]:
    removable = {
        compact("文书内容"),
        compact(metadata.get("court", "")),
        compact(metadata.get("type", "")),
        compact(metadata.get("案号", "")),
    }
    result = list(lines)
    index = 0
    removed_nonblank = 0
    while index < len(result):
        value = compact(strip_gap(result[index]))
        if not value:
            index += 1
            continue
        if value in removable and removed_nonblank < 4:
            del result[index]
            removed_nonblank += 1
            continue
        break
    while result and not result[0].strip():
        result.pop(0)
    return result


def expand_inline_body_lines(lines: list[str], metadata: dict[str, str]) -> list[str]:
    result: list[str] = []
    index = 0
    seen_body = False
    in_signature = False
    while index < len(lines):
        line = lines[index]
        if SIGNATURE_START_RE.match(line) or OLD_SIGNATURE_START_RE.match(line):
            in_signature = True
            result.append(line)
            index += 1
            continue
        if in_signature:
            result.append(line)
            if line.strip() == "}}":
                in_signature = False
            index += 1
            continue

        working = line
        if not seen_body:
            working = strip_inline_intro_prefix(working, metadata)
        body, entries = extract_inline_signature(working)
        body_segments = split_dense_body_line(body) if body != "" else []
        result.extend(body_segments)
        if body_segments and any(segment.strip() for segment in body_segments):
            seen_body = True
        if entries:
            next_index = next_nonblank_index(lines, index + 1)
            if next_index is not None and (
                SIGNATURE_START_RE.match(lines[next_index]) or OLD_SIGNATURE_START_RE.match(lines[next_index])
            ):
                result.extend(entries)
            else:
                result.extend(["{{裁判文书署名|1=", *entries, "}}"])
        index += 1
    return result


def split_dense_body_line(line: str) -> list[str]:
    line = line.replace("\u00a0", " ").strip()
    line = re.sub(r"-PAGE-\s*$", "", line).strip()
    if not line:
        return [""]
    if len(line) < 500:
        return [line]
    segmented = line
    for pattern in INLINE_SEGMENT_PATTERNS:
        segmented = re.sub(rf"(?<!^)(?={pattern})", "\n", segmented)
    return [segment.strip() for segment in segmented.splitlines() if segment.strip()]


def strip_inline_intro_prefix(line: str, metadata: dict[str, str]) -> str:
    prefixes = [
        "文书内容",
        metadata.get("court", ""),
        metadata.get("type", ""),
        metadata.get("案号", ""),
    ]
    candidates = [
        "".join(prefixes),
        "".join(prefixes[1:]),
        "".join(prefixes[2:]),
        prefixes[3],
        "".join(prefixes[1:3]),
        prefixes[1],
        prefixes[2],
        prefixes[0],
    ]
    result = line
    for candidate in sorted({compact(value) for value in candidates if value}, key=len, reverse=True):
        result = remove_compact_prefix(result, candidate)
    return result.lstrip("，。；:：　 ")


def remove_compact_prefix(line: str, compact_prefix: str) -> str:
    compacted, mapping = compact_with_mapping(line)
    if not compact_prefix or not compacted.startswith(compact_prefix):
        return line
    end = mapping[len(compact_prefix) - 1] + 1
    return line[end:]


def extract_inline_signature(line: str) -> tuple[str, list[str]]:
    stripped = line.rstrip()
    if len(stripped) < 20:
        return line, []
    start = find_inline_signature_start(stripped)
    if start is None:
        return line, []
    body = stripped[:start].rstrip("，。；:：　 ")
    tail = stripped[start:]
    entries = parse_inline_signature_entries(tail)
    if not entries:
        return line, []
    return body, entries


def find_inline_signature_start(line: str) -> int | None:
    tail_start = max(0, len(line) - 600)
    tail = line[tail_start:]
    compacted, mapping = compact_with_mapping(tail)
    starts = sorted(
        {
            match.start()
            for role in SIGNATURE_ROLES
            for match in re.finditer(re.escape(role), compacted)
        }
    )
    for role_start in starts:
        rest = compacted[role_start:]
        if len(rest) > 180:
            continue
        entries = parse_inline_signature_entries(rest)
        role_count = sum(1 for entry in entries if "：" in entry)
        has_date = any(is_signature_date_entry(entry) for entry in entries)
        if has_date and role_count >= 2:
            return tail_start + mapping[role_start]
    return None


def is_signature_date_entry(value: str) -> bool:
    return is_date_text(value) or bool(CN_DATE_RE.fullmatch(value) or AR_DATE_RE.fullmatch(value))


def parse_inline_signature_entries(text: str) -> list[str]:
    compacted = compact_signature_tail(text)
    entries: list[str] = []
    index = 0
    while index < len(compacted):
        marker = next_inline_signature_marker(compacted, index)
        if marker is None:
            break
        kind, start, end, value = marker
        if kind == "date":
            entries.append(value)
            index = end
            continue
        next_marker = next_inline_signature_marker(compacted, end)
        next_start = next_marker[1] if next_marker else len(compacted)
        name = compacted[end:next_start].lstrip(":：").strip("，。；:：-")
        if name:
            entries.append(f"{value}：{name}")
        index = next_start
    return dedupe_entries(entries)


def next_inline_signature_marker(text: str, start: int) -> tuple[str, int, int, str] | None:
    matches: list[tuple[str, int, int, str]] = []
    for role in SIGNATURE_ROLES:
        position = text.find(role, start)
        if position >= 0:
            matches.append(("role", position, position + len(role), role))
    date_match = CN_DATE_RE.search(text, start) or AR_DATE_RE.search(text, start)
    if date_match:
        matches.append(("date", date_match.start(), date_match.end(), date_match.group(0)))
    if not matches:
        return None
    return min(matches, key=lambda item: item[1])


def compact_signature_tail(text: str) -> str:
    text = text.replace("-PAGE-", "")
    return "".join(ch for ch in text if not ch.isspace() and ch != "\u00a0")


def compact_with_mapping(text: str) -> tuple[str, list[int]]:
    chars: list[str] = []
    mapping: list[int] = []
    for index, char in enumerate(text):
        if char.isspace() or char == "\u00a0":
            continue
        chars.append(char.replace("(", "（").replace(")", "）"))
        mapping.append(index)
    return "".join(chars), mapping


def next_nonblank_index(lines: list[str], start: int) -> int | None:
    for index in range(start, len(lines)):
        if lines[index].strip():
            return index
    return None


def normalize_standalone_signature_clusters(lines: list[str]) -> list[str]:
    result: list[str] = []
    index = 0
    while index < len(lines):
        if SIGNATURE_START_RE.match(lines[index]) or OLD_SIGNATURE_START_RE.match(lines[index]):
            end = find_template_end(lines, index)
            result.extend(lines[index : end + 1])
            index = end + 1
            continue
        entry = standalone_signature_entry(lines[index])
        if not entry:
            result.append(lines[index])
            index += 1
            continue
        entries = [entry]
        end = index + 1
        while end < len(lines):
            next_entry = standalone_signature_entry(lines[end])
            if not next_entry:
                break
            entries.append(next_entry)
            end += 1
        if is_standalone_signature_cluster(entries):
            result.extend(["{{裁判文书署名|1=", *entries, "}}"])
            index = end
            continue
        result.append(lines[index])
        index += 1
    return result


def standalone_signature_entry(line: str) -> str | None:
    entry = canonical_signature_entry(line)
    if entry:
        return entry
    stripped = strip_gap(line).strip()
    if compact(stripped) == "本件与原本核对无异":
        return stripped
    return None


def is_standalone_signature_cluster(entries: list[str]) -> bool:
    has_role = any("：" in entry for entry in entries)
    has_date = any(is_signature_date_entry(entry) for entry in entries)
    return len(entries) >= 2 and has_role and has_date


def remove_empty_signature_blocks(lines: list[str]) -> list[str]:
    result: list[str] = []
    index = 0
    while index < len(lines):
        if not SIGNATURE_START_RE.match(lines[index]):
            result.append(lines[index])
            index += 1
            continue
        end = find_template_end(lines, index)
        if any(line.strip() for line in lines[index + 1 : end]):
            result.extend(lines[index : end + 1])
        index = end + 1
    return result


def strip_gap(line: str) -> str:
    return line.strip().removeprefix("{{gap}}").strip()


def compact(value: str) -> str:
    return re.sub(r"\s+", "", value or "").replace("(", "（").replace(")", "）")


def normalize_signature_blocks(lines: list[str]) -> list[str]:
    lines = convert_old_signature_blocks(lines)
    index = 0
    while index < len(lines):
        if not SIGNATURE_START_RE.match(lines[index]):
            index += 1
            continue
        end = find_template_end(lines, index)
        existing_entries = canonical_signature_entries(lines[index + 1 : end])
        before_entries, remove_start = collect_preceding_signature_entries(lines, index)
        after_entries, remove_end = collect_following_signature_entries(lines, end + 1)
        entries = dedupe_entries(before_entries + existing_entries + after_entries)
        lines = (
            lines[:remove_start]
            + ["{{裁判文书署名|1=", *entries, "}}"]
            + lines[remove_end:]
        )
        index = remove_start + len(entries) + 2
    return lines


def convert_old_signature_blocks(lines: list[str]) -> list[str]:
    result: list[str] = []
    index = 0
    while index < len(lines):
        if not OLD_SIGNATURE_START_RE.match(lines[index]):
            result.append(lines[index])
            index += 1
            continue
        end = find_template_end(lines, index)
        entries = canonical_signature_entries(
            [line for line in lines[index + 1 : end] if not line.strip().startswith("{{印|")]
        )
        result.extend(["{{裁判文书署名|1=", *entries, "}}"])
        index = end + 1
    return result


def find_template_end(lines: list[str], start: int) -> int:
    for index in range(start + 1, len(lines)):
        if lines[index].strip() == "}}":
            return index
    raise RuntimeError("unterminated signature template")


def collect_preceding_signature_entries(lines: list[str], signature_index: int) -> tuple[list[str], int]:
    entries_reversed: list[str] = []
    index = signature_index - 1
    while index >= 0:
        stripped = lines[index].strip()
        if not stripped:
            index -= 1
            continue
        entry = canonical_signature_entry(lines[index])
        if not entry:
            break
        entries_reversed.append(entry)
        index -= 1
    entries_reversed.reverse()
    return entries_reversed, index + 1


def collect_following_signature_entries(lines: list[str], start: int) -> tuple[list[str], int]:
    entries: list[str] = []
    index = start
    while index < len(lines):
        stripped = lines[index].strip()
        if not stripped:
            index += 1
            continue
        if PRC_EXEMPT_RE.match(stripped) or stripped.startswith("[[Category:"):
            break
        entry = canonical_signature_entry(lines[index])
        if not entry:
            break
        entries.append(entry)
        index += 1
    return entries, index


def canonical_signature_entries(raw_lines: Iterable[str]) -> list[str]:
    entries: list[str] = []
    for line in raw_lines:
        entry = canonical_signature_entry(line)
        if entry:
            entries.append(entry)
    return dedupe_entries(entries)


def canonical_signature_entry(line: str) -> str | None:
    stripped = strip_gap(line)
    stripped = strip_signature_leading_junk(stripped)
    stripped = re.sub(r"\s+", "", stripped)
    if not stripped:
        return None
    if is_signature_date_entry(stripped):
        return stripped
    parsed = parse_signature_entries(stripped)
    if parsed:
        return "　".join(f"{job}：{name}" for job, name in parsed)
    match = re.match(r"^(审判长|审判员|人民陪审员|法官助理|书记员|代书记员)[:：]?(.+)$", stripped)
    if match:
        return f"{match.group(1)}：{match.group(2)}"
    return None


def dedupe_entries(entries: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for entry in entries:
        if entry in seen:
            continue
        seen.add(entry)
        result.append(entry)
    return result


def gap_body_lines(lines: list[str]) -> list[str]:
    result: list[str] = []
    in_signature = False
    for line in lines:
        stripped = line.strip()
        if SIGNATURE_START_RE.match(line):
            in_signature = True
            result.append("{{裁判文书署名|1=")
            continue
        if in_signature:
            result.append(line)
            if stripped == "}}":
                in_signature = False
            continue
        if not stripped:
            result.append("")
            continue
        if (
            stripped.startswith("{{")
            or stripped.startswith("[[")
            or stripped.startswith("#REDIRECT")
            or stripped.startswith("<")
        ):
            result.append(line)
            continue
        result.append(f"{{{{gap}}}}{stripped}")
    return ensure_body_paragraph_breaks(trim_blank_lines(result))


def ensure_body_paragraph_breaks(lines: list[str]) -> list[str]:
    result: list[str] = []
    previous_kind = ""
    in_signature = False
    for line in lines:
        stripped = line.strip()
        if SIGNATURE_START_RE.match(line):
            if previous_kind == "body" and result and result[-1].strip():
                result.append("")
            in_signature = True
            result.append(line)
            previous_kind = "signature"
            continue
        if in_signature:
            result.append(line)
            if stripped == "}}":
                in_signature = False
                previous_kind = "signature"
            continue
        if not stripped:
            if result and result[-1].strip():
                result.append("")
            previous_kind = ""
            continue
        kind = "body" if stripped.startswith("{{gap}}") else "other"
        if kind == "body" and previous_kind in {"body", "signature"} and result and result[-1].strip():
            result.append("")
        result.append(line)
        previous_kind = kind
    return trim_blank_lines(result)


def trim_blank_lines(lines: list[str]) -> list[str]:
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return lines


def visible_text(text: str) -> str:
    text = re.sub(r"\{\{Header/裁判文书.*?\n\}\}", "", text, flags=re.S | re.I)
    text = re.sub(r"\{\{header.*?\n\}\}", "", text, flags=re.S | re.I)
    text = text.replace("{{gap}}", "\n")
    text = text.replace("文书内容", "\n")
    return text


def infer_court(text: str) -> str:
    for match in COURT_RE.finditer(text[:2000]):
        value = normalize_court_name(match.group(1).replace("文书内容", ""))
        if value:
            return value
    return ""


def infer_case_number(text: str) -> str:
    bad_context = ("车架号", "房产证号", "发票号", "账号", "合同编号", "身份证号", "统一社会信用代码")
    for match in CASE_NUMBER_RE.finditer(text[:4000]):
        start = max(0, match.start() - 10)
        context = text[start : match.end() + 10]
        if any(marker in context for marker in bad_context):
            continue
        value = normalize_case_number(match.group(0))
        if value:
            return value
    return ""


def infer_doc_type(text: str, title: str) -> str:
    haystack = f"{title}\n{text[:1000]}"
    for doc_type in DOC_TYPES:
        if doc_type in haystack:
            return doc_type
    if "民事令" in haystack or "支付令" in haystack:
        return "支付令"
    return ""


CN_DIGITS = {"〇": 0, "○": 0, "零": 0, "一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}


def infer_date(text: str) -> tuple[str, str, str]:
    match = CN_DATE_RE.search(text)
    if match:
        year = "".join(str(CN_DIGITS[ch]) for ch in match.group(1) if ch in CN_DIGITS)
        month = parse_cn_number(match.group(2))
        day = parse_cn_number(match.group(3))
        if len(year) == 4 and month and day:
            return year, str(month), str(day)
    match = AR_DATE_RE.search(text)
    if match:
        return match.group(1), str(int(match.group(2))), str(int(match.group(3)))
    return "", "", ""


def parse_cn_number(value: str) -> int | None:
    if value in CN_DIGITS:
        return CN_DIGITS[value]
    if value == "十":
        return 10
    if value.startswith("十") and len(value) == 2:
        return 10 + CN_DIGITS.get(value[1], 0)
    if value.endswith("十") and len(value) == 2:
        return CN_DIGITS.get(value[0], 0) * 10
    if "十" in value:
        left, right = value.split("十", 1)
        return (CN_DIGITS.get(left, 1) if left else 1) * 10 + (CN_DIGITS.get(right, 0) if right else 0)
    return None


def infer_location(court: str) -> str:
    for suffix in ("省", "自治区", "市"):
        index = court.find(suffix)
        if index > 0:
            return court[: index + len(suffix)]
    return ""


def apply_special_merge_notes(title: str, text: str, imports: list[RevisionData]) -> str:
    if title == "吴海君、徐琴琴民间借贷纠纷一审民事判决书":
        text = text.replace("二〇二四年三月二十六日", "{{另|二〇二四年三月二十六日|二〇二四年三月二十八日}}")
    return text


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--interval", type=float, default=3.0)
    parser.add_argument("--maxlag", type=int, default=5)
    parser.add_argument("--only", action="append", default=[])
    parser.add_argument("--report", type=Path, default=PROJECT_ROOT / "working" / "output" / "unchecked_overwrite_resolve_report.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configure_throttle(interval=args.interval, maxlag=args.maxlag)
    resolver = Resolver(
        dry_run=args.dry_run,
        interval=args.interval,
        maxlag=args.maxlag,
        only=set(args.only) if args.only else None,
    )
    resolver.run()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(resolver.report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nReport: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
