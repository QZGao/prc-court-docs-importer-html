"""
Location inference from court names.

This module provides functionality to infer the location (province/region) from 
a court name. Chinese courts often omit the province prefix at certain levels,
so we need to infer it from the full court hierarchy or the court name itself.
"""

import re
from typing import Optional


# Direct province/region mappings from court name prefixes
# These are for courts that explicitly include the province/region
PROVINCE_PREFIXES = [
    "北京市",
    "天津市", 
    "上海市",
    "重庆市",
    "河北省",
    "山西省",
    "辽宁省",
    "吉林省",
    "黑龙江省",
    "江苏省",
    "浙江省",
    "安徽省",
    "福建省",
    "江西省",
    "山东省",
    "河南省",
    "湖北省",
    "湖南省",
    "广东省",
    "海南省",
    "四川省",
    "贵州省",
    "云南省",
    "陕西省",
    "甘肃省",
    "青海省",
    "台湾省",
    "内蒙古自治区",
    "广西壮族自治区",
    "西藏自治区",
    "宁夏回族自治区",
    "新疆维吾尔自治区",
    "香港特别行政区",
    "澳门特别行政区",
]

# Short forms mapping to full province names
PROVINCE_SHORT_FORMS = {
    "京": "北京市",
    "津": "天津市",
    "沪": "上海市",
    "渝": "重庆市",
    "冀": "河北省",
    "晋": "山西省",
    "辽": "辽宁省",
    "吉": "吉林省",
    "黑": "黑龙江省",
    "苏": "江苏省",
    "浙": "浙江省",
    "皖": "安徽省",
    "闽": "福建省",
    "赣": "江西省",
    "鲁": "山东省",
    "豫": "河南省",
    "鄂": "湖北省",
    "湘": "湖南省",
    "粤": "广东省",
    "琼": "海南省",
    "川": "四川省",
    "蜀": "四川省",
    "贵": "贵州省",
    "黔": "贵州省",
    "云": "云南省",
    "滇": "云南省",
    "陕": "陕西省",
    "秦": "陕西省",
    "甘": "甘肃省",
    "陇": "甘肃省",
    "青": "青海省",
    "台": "台湾省",
    "蒙": "内蒙古自治区",
    "桂": "广西壮族自治区",
    "藏": "西藏自治区",
    "宁": "宁夏回族自治区",
    "新": "新疆维吾尔自治区",
    "港": "香港特别行政区",
    "澳": "澳门特别行政区",
}

# City-level mappings for cases where province is omitted but city can identify province
# This is a subset of major cities
CITY_TO_PROVINCE = {
    # Municipalities (direct mapping)
    "北京": "北京市",
    "天津": "天津市",
    "上海": "上海市",
    "重庆": "重庆市",
    
    # Hebei
    "石家庄": "河北省",
    "唐山": "河北省",
    "秦皇岛": "河北省",
    "邯郸": "河北省",
    "邢台": "河北省",
    "保定": "河北省",
    "张家口": "河北省",
    "承德": "河北省",
    "沧州": "河北省",
    "廊坊": "河北省",
    "衡水": "河北省",
    
    # Shanxi
    "太原": "山西省",
    "大同": "山西省",
    "阳泉": "山西省",
    "长治": "山西省",
    "晋城": "山西省",
    "朔州": "山西省",
    "晋中": "山西省",
    "运城": "山西省",
    "忻州": "山西省",
    "临汾": "山西省",
    "吕梁": "山西省",
    
    # Liaoning
    "沈阳": "辽宁省",
    "大连": "辽宁省",
    "鞍山": "辽宁省",
    "抚顺": "辽宁省",
    "本溪": "辽宁省",
    "丹东": "辽宁省",
    "锦州": "辽宁省",
    "营口": "辽宁省",
    "阜新": "辽宁省",
    "辽阳": "辽宁省",
    "盘锦": "辽宁省",
    "铁岭": "辽宁省",
    "朝阳": "辽宁省",
    "葫芦岛": "辽宁省",
    
    # Jilin
    "长春": "吉林省",
    "吉林": "吉林省",
    "四平": "吉林省",
    "辽源": "吉林省",
    "通化": "吉林省",
    "白山": "吉林省",
    "松原": "吉林省",
    "白城": "吉林省",
    "延边": "吉林省",
    
    # Heilongjiang
    "哈尔滨": "黑龙江省",
    "齐齐哈尔": "黑龙江省",
    "牡丹江": "黑龙江省",
    "佳木斯": "黑龙江省",
    "大庆": "黑龙江省",
    "鸡西": "黑龙江省",
    "双鸭山": "黑龙江省",
    "伊春": "黑龙江省",
    "七台河": "黑龙江省",
    "鹤岗": "黑龙江省",
    "黑河": "黑龙江省",
    "绥化": "黑龙江省",
    
    # Jiangsu
    "南京": "江苏省",
    "无锡": "江苏省",
    "徐州": "江苏省",
    "常州": "江苏省",
    "苏州": "江苏省",
    "南通": "江苏省",
    "连云港": "江苏省",
    "淮安": "江苏省",
    "盐城": "江苏省",
    "扬州": "江苏省",
    "镇江": "江苏省",
    "泰州": "江苏省",
    "宿迁": "江苏省",
    
    # Zhejiang
    "杭州": "浙江省",
    "宁波": "浙江省",
    "温州": "浙江省",
    "嘉兴": "浙江省",
    "湖州": "浙江省",
    "绍兴": "浙江省",
    "金华": "浙江省",
    "衢州": "浙江省",
    "舟山": "浙江省",
    "台州": "浙江省",
    "丽水": "浙江省",
    
    # Anhui
    "合肥": "安徽省",
    "芜湖": "安徽省",
    "蚌埠": "安徽省",
    "淮南": "安徽省",
    "马鞍山": "安徽省",
    "淮北": "安徽省",
    "铜陵": "安徽省",
    "安庆": "安徽省",
    "黄山": "安徽省",
    "滁州": "安徽省",
    "阜阳": "安徽省",
    "宿州": "安徽省",
    "六安": "安徽省",
    "亳州": "安徽省",
    "池州": "安徽省",
    "宣城": "安徽省",
    
    # Fujian
    "福州": "福建省",
    "厦门": "福建省",
    "莆田": "福建省",
    "三明": "福建省",
    "泉州": "福建省",
    "漳州": "福建省",
    "南平": "福建省",
    "龙岩": "福建省",
    "宁德": "福建省",
    
    # Jiangxi
    "南昌": "江西省",
    "景德镇": "江西省",
    "萍乡": "江西省",
    "九江": "江西省",
    "新余": "江西省",
    "鹰潭": "江西省",
    "赣州": "江西省",
    "吉安": "江西省",
    "宜春": "江西省",
    "抚州": "江西省",
    "上饶": "江西省",
    
    # Shandong
    "济南": "山东省",
    "青岛": "山东省",
    "淄博": "山东省",
    "枣庄": "山东省",
    "东营": "山东省",
    "烟台": "山东省",
    "潍坊": "山东省",
    "济宁": "山东省",
    "泰安": "山东省",
    "威海": "山东省",
    "日照": "山东省",
    "临沂": "山东省",
    "德州": "山东省",
    "聊城": "山东省",
    "滨州": "山东省",
    "菏泽": "山东省",
    
    # Henan
    "郑州": "河南省",
    "开封": "河南省",
    "洛阳": "河南省",
    "平顶山": "河南省",
    "安阳": "河南省",
    "鹤壁": "河南省",
    "新乡": "河南省",
    "焦作": "河南省",
    "濮阳": "河南省",
    "许昌": "河南省",
    "漯河": "河南省",
    "三门峡": "河南省",
    "南阳": "河南省",
    "商丘": "河南省",
    "信阳": "河南省",
    "周口": "河南省",
    "驻马店": "河南省",
    
    # Hubei
    "武汉": "湖北省",
    "黄石": "湖北省",
    "十堰": "湖北省",
    "宜昌": "湖北省",
    "襄阳": "湖北省",
    "鄂州": "湖北省",
    "荆门": "湖北省",
    "孝感": "湖北省",
    "荆州": "湖北省",
    "黄冈": "湖北省",
    "咸宁": "湖北省",
    "随州": "湖北省",
    "恩施": "湖北省",
    
    # Hunan
    "长沙": "湖南省",
    "株洲": "湖南省",
    "湘潭": "湖南省",
    "衡阳": "湖南省",
    "邵阳": "湖南省",
    "岳阳": "湖南省",
    "常德": "湖南省",
    "张家界": "湖南省",
    "益阳": "湖南省",
    "郴州": "湖南省",
    "永州": "湖南省",
    "怀化": "湖南省",
    "娄底": "湖南省",
    "湘西": "湖南省",
    
    # Guangdong
    "广州": "广东省",
    "韶关": "广东省",
    "深圳": "广东省",
    "珠海": "广东省",
    "汕头": "广东省",
    "佛山": "广东省",
    "江门": "广东省",
    "湛江": "广东省",
    "茂名": "广东省",
    "肇庆": "广东省",
    "惠州": "广东省",
    "梅州": "广东省",
    "汕尾": "广东省",
    "河源": "广东省",
    "阳江": "广东省",
    "清远": "广东省",
    "东莞": "广东省",
    "中山": "广东省",
    "潮州": "广东省",
    "揭阳": "广东省",
    "云浮": "广东省",
    
    # Hainan
    "海口": "海南省",
    "三亚": "海南省",
    "三沙": "海南省",
    "儋州": "海南省",
    
    # Sichuan
    "成都": "四川省",
    "自贡": "四川省",
    "攀枝花": "四川省",
    "泸州": "四川省",
    "德阳": "四川省",
    "绵阳": "四川省",
    "广元": "四川省",
    "遂宁": "四川省",
    "内江": "四川省",
    "乐山": "四川省",
    "南充": "四川省",
    "眉山": "四川省",
    "宜宾": "四川省",
    "广安": "四川省",
    "达州": "四川省",
    "雅安": "四川省",
    "巴中": "四川省",
    "资阳": "四川省",
    "阿坝": "四川省",
    "甘孜": "四川省",
    "凉山": "四川省",
    
    # Guizhou
    "贵阳": "贵州省",
    "六盘水": "贵州省",
    "遵义": "贵州省",
    "安顺": "贵州省",
    "毕节": "贵州省",
    "铜仁": "贵州省",
    "黔西南": "贵州省",
    "黔东南": "贵州省",
    "黔南": "贵州省",
    
    # Yunnan
    "昆明": "云南省",
    "曲靖": "云南省",
    "玉溪": "云南省",
    "保山": "云南省",
    "昭通": "云南省",
    "丽江": "云南省",
    "普洱": "云南省",
    "临沧": "云南省",
    "楚雄": "云南省",
    "红河": "云南省",
    "文山": "云南省",
    "西双版纳": "云南省",
    "大理": "云南省",
    "德宏": "云南省",
    "怒江": "云南省",
    "迪庆": "云南省",
    
    # Shaanxi
    "西安": "陕西省",
    "铜川": "陕西省",
    "宝鸡": "陕西省",
    "咸阳": "陕西省",
    "渭南": "陕西省",
    "延安": "陕西省",
    "汉中": "陕西省",
    "榆林": "陕西省",
    "安康": "陕西省",
    "商洛": "陕西省",
    
    # Gansu
    "兰州": "甘肃省",
    "嘉峪关": "甘肃省",
    "金昌": "甘肃省",
    "白银": "甘肃省",
    "天水": "甘肃省",
    "武威": "甘肃省",
    "张掖": "甘肃省",
    "平凉": "甘肃省",
    "酒泉": "甘肃省",
    "庆阳": "甘肃省",
    "定西": "甘肃省",
    "陇南": "甘肃省",
    "临夏": "甘肃省",
    "甘南": "甘肃省",
    
    # Qinghai
    "西宁": "青海省",
    "海东": "青海省",
    "海北": "青海省",
    "黄南": "青海省",
    "海南州": "青海省",
    "果洛": "青海省",
    "玉树": "青海省",
    "海西": "青海省",
    
    # Inner Mongolia
    "呼和浩特": "内蒙古自治区",
    "包头": "内蒙古自治区",
    "乌海": "内蒙古自治区",
    "赤峰": "内蒙古自治区",
    "通辽": "内蒙古自治区",
    "鄂尔多斯": "内蒙古自治区",
    "呼伦贝尔": "内蒙古自治区",
    "巴彦淖尔": "内蒙古自治区",
    "乌兰察布": "内蒙古自治区",
    "兴安盟": "内蒙古自治区",
    "锡林郭勒": "内蒙古自治区",
    "阿拉善": "内蒙古自治区",
    
    # Guangxi
    "南宁": "广西壮族自治区",
    "柳州": "广西壮族自治区",
    "桂林": "广西壮族自治区",
    "梧州": "广西壮族自治区",
    "北海": "广西壮族自治区",
    "防城港": "广西壮族自治区",
    "钦州": "广西壮族自治区",
    "贵港": "广西壮族自治区",
    "玉林": "广西壮族自治区",
    "百色": "广西壮族自治区",
    "贺州": "广西壮族自治区",
    "河池": "广西壮族自治区",
    "来宾": "广西壮族自治区",
    "崇左": "广西壮族自治区",
    
    # Tibet
    "拉萨": "西藏自治区",
    "日喀则": "西藏自治区",
    "昌都": "西藏自治区",
    "林芝": "西藏自治区",
    "山南": "西藏自治区",
    "那曲": "西藏自治区",
    "阿里": "西藏自治区",
    
    # Ningxia
    "银川": "宁夏回族自治区",
    "石嘴山": "宁夏回族自治区",
    "吴忠": "宁夏回族自治区",
    "固原": "宁夏回族自治区",
    "中卫": "宁夏回族自治区",
    
    # Xinjiang
    "乌鲁木齐": "新疆维吾尔自治区",
    "克拉玛依": "新疆维吾尔自治区",
    "吐鲁番": "新疆维吾尔自治区",
    "哈密": "新疆维吾尔自治区",
    "昌吉": "新疆维吾尔自治区",
    "博尔塔拉": "新疆维吾尔自治区",
    "巴音郭楞": "新疆维吾尔自治区",
    "阿克苏": "新疆维吾尔自治区",
    "克孜勒苏": "新疆维吾尔自治区",
    "喀什": "新疆维吾尔自治区",
    "和田": "新疆维吾尔自治区",
    "伊犁": "新疆维吾尔自治区",
    "塔城": "新疆维吾尔自治区",
    "阿勒泰": "新疆维吾尔自治区",
    "奇台": "新疆维吾尔自治区",
}


def infer_location_from_court(court_name: str) -> Optional[str]:
    """
    Infer the location (province/region) from a court name.
    
    Args:
        court_name: The name of the court (e.g., "南昌市红谷滩区人民法院")
        
    Returns:
        The inferred location string, or None if cannot be determined.
        
    Examples:
        >>> infer_location_from_court("江西省南昌市红谷滩区人民法院")
        "江西省南昌市"
        >>> infer_location_from_court("南昌市红谷滩区人民法院")
        "江西省南昌市"
        >>> infer_location_from_court("鄂托克前旗人民法院")
        "内蒙古自治区"
    """
    if not court_name:
        return None
    
    # Clean up whitespace in court name
    court_name = re.sub(r'\s+', '', court_name)
    
    # First, check if the court name already contains a province prefix
    for province in PROVINCE_PREFIXES:
        if court_name.startswith(province):
            # Extract city if present
            remainder = court_name[len(province):]
            # Look for city pattern: XX市 or XX州 or XX盟
            city_match = re.match(r'([^市州盟]+(?:市|州|盟|地区))', remainder)
            if city_match:
                return province + city_match.group(1)
            return province
    
    # Second, try to identify by city name in the court name
    for city, province in CITY_TO_PROVINCE.items():
        if city in court_name:
            # For municipalities, return just the municipality
            if province in ["北京市", "天津市", "上海市", "重庆市"]:
                return province
            # For regular provinces, return province + city
            return province + city + ("市" if not city.endswith(("州", "盟", "地区")) else "")
    
    # Third, check for autonomous region/prefecture indicators
    if "自治州" in court_name:
        # Try to identify the autonomous prefecture
        for city, province in CITY_TO_PROVINCE.items():
            if city in court_name:
                return province
    
    # Fourth, check for special administrative regions
    if "旗" in court_name:
        # This is likely Inner Mongolia
        return "内蒙古自治区"
    
    # Fifth, try to extract from parent court hierarchy (s22 contains full hierarchy)
    # This would need the full s22 field
    
    return None


def infer_location_from_s22(s22: str) -> Optional[str]:
    """
    Infer location from the s22 field which contains court name with province.
    
    Args:
        s22: The s22 field containing court + type + docID
        
    Returns:
        The inferred location string.
    """
    if not s22:
        return None
    
    # s22 format is: "Court\nDocType\nDocID" or similar
    # The first line contains the full court name with province
    lines = s22.strip().split('\n')
    if lines:
        return infer_location_from_court(lines[0])
    
    return None


def extract_province_from_court(court_name: str) -> Optional[str]:
    """
    Extract just the province/region from a court name.
    
    Args:
        court_name: The full court name
        
    Returns:
        Just the province name (e.g., "江西省")
    """
    if not court_name:
        return None
    
    court_name = re.sub(r'\s+', '', court_name)
    
    # Check direct prefix
    for province in PROVINCE_PREFIXES:
        if court_name.startswith(province):
            return province
    
    # Try city lookup
    for city, province in CITY_TO_PROVINCE.items():
        if city in court_name:
            return province
    
    # Check for Inner Mongolia flags (旗)
    if "旗" in court_name:
        return "内蒙古自治区"
    
    return None
