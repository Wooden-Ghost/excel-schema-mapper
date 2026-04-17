"""
终极表格汇总处理程序 v2.9.4（PySide6 + pandas/openpyxl）
========================================================

本版本改动（v2.8）
- ✅ 窗口标题显示版本号：v2.8
- ✅ 列映射区改为“横向表格”并支持自定义列宽（映射框设置）
- ✅ 新增“映射框设置”按钮：
  - 按列顺序显示所有输出列
  - 支持：设置每列宽度、启用/禁用、删除、添加新列
  - 支持：设置每列默认值
  - 支持：设置该列模式（自动映射 / 手动输入 / 固定默认 / 留空）
  - 系统记忆这些设置（QSettings）
- ✅ “数据来源”移入列配置：默认模式为【手动输入】，在映射区可直接输入
- ✅ 不再生成本地日志文件（仅在界面日志框显示）
- ✅ 按钮颜色加深、Hover/Pressed 更明显，整体更清爽

使用方法
1) pip install PySide6 pandas openpyxl
2) python ultimate_excel_processor_v2_5.py

注意
- “客户名称”必须启用且模式为【自动映射】，否则不允许开始处理。
- 重量KG仍严格要求源列名同时包含 weight + kg（或 “重量”+“kg/公斤”）。LB/POUND 直接忽略。
"""

from __future__ import annotations

import json
import re
import sys
import time
import traceback
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from PySide6.QtCore import QSettings, Qt, QThread, QObject, Signal, QUrl
from PySide6.QtGui import QDesktopServices, QAction
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QInputDialog,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QRadioButton,
    QSizePolicy,
    QSpacerItem,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QTabWidget,
    QTextBrowser
)


# --------------------------- 常量与默认列 ---------------------------

DEFAULT_OUTPUT_COLUMNS = [
    "客户名称",
    "最近货运日期",
    "重量KG",
    "公司官网",
    "公司简介",
    "海关提单数",
    "产品明细",
    "HSCODE商品描述",
    "合作供应商1",
    "合作供应商2",
    "合作供应商3",
    "国家",
    "负责人",
    "是否做背调",
    "客户优先级",
    "数据来源",
    "交易总额",
]

RESPONSIBLE_NAME = "陈柏安"

# 可自动映射的字段（如果输出列里包含这些名字，且该列模式=自动映射，则提供候选源列）
AUTO_MAP_FIELDS = [
    "客户名称",
    "最近货运日期",
    "重量KG",
    "公司官网",
    "公司简介",
    "海关提单数",
    "产品明细",
    "HSCODE商品描述",
    "国家",
    "交易总额",
    "合作供应商1",
    "合作供应商2",
    "合作供应商3",
    "数据来源",
]

REQUIRED_AUTO_MAP_FIELD = "客户名称"

HEADER_SCAN_ROWS = 50
SAMPLE_PREVIEW_N = 5
MAX_CANDIDATES = 18

# 模式枚举
MODE_AUTO = "自动映射"
MODE_MANUAL = "手动输入"
MODE_DEFAULT = "固定默认"
MODE_EMPTY = "留空"
MODE_CHOICES = [MODE_AUTO, MODE_MANUAL, MODE_DEFAULT, MODE_EMPTY]

# 关键词规则：用于自动推荐候选列（列名英文/中文都参与）
# 说明：
# - 支持负权重（用于“排除特征”），例如 address/phone/id
# - 候选列下拉只展示 score>0 的列（0 或负分视为完全无关）
DEFAULT_KEYWORDS: Dict[str, List[Tuple[str, int]]] = {
    "客户名称": [
        ("importer", 5),
        ("buyer", 4),
        ("address", -5),
        ("phone", -5),
        ("客户名称", 10),
        ("consignee", 3),
        ("id", -5),
    ],
    "最近货运日期": [
        ("arrival", 6),
        ("arrive", 6),
        ("最近货运日期", 10),
        ("date", 4),
    ],
    "重量KG": [
        ("weight", 3),
        ("重量", 3),
        ("kg", 3),
        ("公斤", 3),
    ],
    "海关提单数": [
        ("shipments", 7),
        ("shipment", 7),
        ("海关提单数", 10),
    ],
    "产品明细": [
        ("产品明细", 8),
    ],
    "HSCODE商品描述": [
        ("description", 6),
        ("descript", 4),
        ("product", 6),
        ("商品描述", 6),
        ("Hs CODE商品描述", 6),
        ("HSCODE商品描述", 6),
    ],
    "国家": [
        ("国家", 7),
    ],
    "交易总额": [
        ("交易总额", 7),
    ],
    "公司官网": [
        ("公司官网", 7),
    ],
    "公司简介": [
        ("公司简介", 7),
    ],
    "数据来源": [
        ("数据来源", 10),
    ],
    # 默认补齐：合作供应商字段（你可在“关键字设置”里自行调整/删除）
    "合作供应商1": [
        ("合作供应商1", 10),
        ("supplier1", 6),
        ("vendor1", 6),
        ("supplier", 2),
        ("vendor", 2),
    ],
    "合作供应商2": [
        ("合作供应商2", 10),
        ("supplier2", 6),
        ("vendor2", 6),
        ("supplier", 2),
        ("vendor", 2),
    ],
    "合作供应商3": [
        ("合作供应商3", 10),
        ("supplier3", 6),
        ("vendor3", 6),
        ("supplier", 2),
        ("vendor", 2),
    ],
}

# 运行时关键词（可被用户在 UI 中修改并持久化）
KEYWORDS: Dict[str, List[Tuple[str, int]]] = json.loads(json.dumps(DEFAULT_KEYWORDS, ensure_ascii=False))

KEYWORDS_SETTINGS_KEY = "keywords_json_v1"

def _normalize_keywords_dict(d: Any) -> Dict[str, List[Tuple[str, int]]]:
    """确保关键词结构合法：{field: [(kw, weight), ...]}，并过滤掉空关键词。"""
    out: Dict[str, List[Tuple[str, int]]] = {}
    if not isinstance(d, dict):
        return json.loads(json.dumps(DEFAULT_KEYWORDS, ensure_ascii=False))
    for field, items in d.items():
        if not field or not isinstance(items, list):
            continue
        new_items: List[Tuple[str, int]] = []
        for it in items:
            if not isinstance(it, (list, tuple)) or len(it) < 2:
                continue
            kw = safe_str(it[0])
            if not kw:
                continue
            try:
                w = int(it[1])
            except Exception:
                continue
            new_items.append((kw, w))
        if new_items:
            out[field] = new_items

    # 保底：缺失字段用默认补齐（保持你“最新标准”为默认来源）
    for k, v in DEFAULT_KEYWORDS.items():
        out.setdefault(k, list(v))
    return out


def load_keywords_from_settings(settings: QSettings) -> Dict[str, List[Tuple[str, int]]]:
    raw = settings.value(KEYWORDS_SETTINGS_KEY, "")
    if not raw:
        return json.loads(json.dumps(DEFAULT_KEYWORDS, ensure_ascii=False))
    try:
        obj = json.loads(raw)
        return _normalize_keywords_dict(obj)
    except Exception:
        return json.loads(json.dumps(DEFAULT_KEYWORDS, ensure_ascii=False))


def save_keywords_to_settings(settings: QSettings, keywords: Dict[str, List[Tuple[str, int]]]) -> None:
    settings.setValue(KEYWORDS_SETTINGS_KEY, json.dumps(keywords, ensure_ascii=False, indent=2))


# --------------------------- 数据结构 ---------------------------

@dataclass
class FileAnalysis:
    path: Path
    header_row: int
    columns: List[str]
    fingerprint: str


@dataclass
class GroupAnalysis:
    fingerprint: str
    columns: List[str]
    files: List[FileAnalysis]


# --------------------------- 工具函数 ---------------------------

def file_base_name(path: Path) -> str:
    """
    数据来源（文件名）提取：去掉常见的数字后缀，如
    - 深圳市柏星龙-1-230 -> 深圳市柏星龙
    - ABC_2_61 -> ABC
    """
    stem = path.stem
    stem = re.sub(r"[-_]\d+[-_]\d+$", "", stem)
    stem = re.sub(r"[-_]\d+$", "", stem)
    return stem.strip()

def desktop_dir() -> Path:
    home = Path.home()
    cand = home / "Desktop"
    return cand if cand.exists() else home


def safe_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and pd.isna(x):
        return ""
    return str(x).strip()


def normalize_spaces(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s


def company_norm_key(s: str) -> str:
    """弱规范化：去首尾空格、合并空格、统一大写"""
    s = normalize_spaces(s)
    return s.upper()


def parse_number_series(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace(",", "", regex=False).str.strip()
    s = s.replace({"": None, "nan": None, "None": None})
    return pd.to_numeric(s, errors="coerce")


def parse_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def dedup_join(values: List[str], sep: str) -> str:
    seen = set()
    out = []
    for v in values:
        v2 = normalize_spaces(v)
        if not v2:
            continue
        if v2 in seen:
            continue
        seen.add(v2)
        out.append(v2)
    return sep.join(out)


def columns_fingerprint(cols: List[str]) -> str:
    norm = [normalize_spaces(c).lower() for c in cols]
    payload = "|".join(norm) + f"::n={len(norm)}"
    return sha1(payload.encode("utf-8")).hexdigest()


def _is_date_like(s: str) -> bool:
    s = s.strip()
    if re.fullmatch(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", s):
        return True
    if re.fullmatch(r"\d{1,2}[-/]\d{1,2}[-/]\d{2,4}", s):
        return True
    return False


def header_score(row_values: List[str]) -> int:
    """
    更稳健的表头评分：
    - 强加分：命中“锚点词” consginee/importer/shipments/date/weight/kg 等
    - 惩罚：超长文本、很多分号、很多看起来像日期的值（常见数据行）
    """
    vals = [safe_str(v) for v in row_values]
    non_empty = [v for v in vals if v]
    score = 0

    score += min(len(non_empty), 30)

    joined = " ".join(non_empty).lower()

    anchors = ["consignee", "importer", "shipments", "shipment", "date", "arrival", "weight", "kg", "hscode", "hs code"]
    for a in anchors:
        if a in joined:
            score += 12

    for field, kws in KEYWORDS.items():
        for kw, w in kws:
            kw2 = safe_str(kw).lower()
            if kw2 and kw2 in joined:
                score += w

    long_cells = sum(1 for v in non_empty if len(v) >= 35)
    score -= long_cells * 6

    semi = joined.count(";")
    if semi >= 2:
        score -= min(20, semi * 3)

    date_like = sum(1 for v in non_empty[:10] if _is_date_like(v))
    score -= date_like * 8

    suffix_hits = sum(1 for sfx in [" ltd", " inc", " llc", " co.", " company", " limited"] if sfx in joined)
    score -= suffix_hits * 2

    return score


def looks_like_footer(row: pd.Series) -> bool:
    vals = [safe_str(v).lower() for v in row.tolist()]
    non_empty = sum(1 for v in vals if v)
    joined = " ".join(vals)
    if non_empty <= 2 and (("total" in joined) or ("合计" in joined) or ("grand total" in joined)):
        return True
    if non_empty == 1 and re.fullmatch(r"[-=_\s]+", vals[0] or ""):
        return True
    return False


def read_raw_table(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in [".xlsx", ".xlsm", ".xls"]:
        return pd.read_excel(path, sheet_name=0, header=None, engine="openpyxl")
    elif ext == ".csv":
        return pd.read_csv(path, header=None, encoding_errors="ignore")
    else:
        raise ValueError(f"不支持的文件类型：{ext}（仅支持 xlsx/xlsm/xls/csv）")


def detect_header_row(raw: pd.DataFrame, max_scan: int = HEADER_SCAN_ROWS) -> int:
    scan_n = min(max_scan, len(raw))
    best_i = 0
    best_score = -10**9
    for i in range(scan_n):
        row = raw.iloc[i].tolist()
        row_vals = [safe_str(v) for v in row]
        score = header_score(row_vals)
        score -= i * 0.05  # 同分时偏向更靠上
        if score > best_score:
            best_score = score
            best_i = i
    return best_i


def build_table_from_raw(raw: pd.DataFrame, header_row: int) -> Tuple[pd.DataFrame, List[str]]:
    header = raw.iloc[header_row].tolist()
    cols = []
    for idx, v in enumerate(header):
        name = safe_str(v)
        if not name:
            name = f"UNNAMED_{idx}"
        cols.append(normalize_spaces(name))
    data = raw.iloc[header_row + 1 :].copy()
    data.columns = cols
    return data, cols


def keyword_rank(col_name: str, field: str) -> int:
    name = col_name.lower()
    score = 0
    for kw, w in KEYWORDS.get(field, []):
        kw2 = safe_str(kw).lower()
        if kw2 and kw2 in name:
            score += w
    if field == "客户名称" and "importer entity" in name:
        score -= 1
    return score


def strict_weightkg(col_name: str) -> bool:
    n = col_name.lower()
    return ("weight" in n or "重量" in n) and ("kg" in n or "公斤" in n)


def suggest_mapping(columns: List[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    # 国家：更严格——只有“国家”（仅这两个汉字）才给出默认建议，否则留空（避免误映射）
    has_cn_country = any(normalize_spaces(c) == "国家" for c in columns)

    for field in AUTO_MAP_FIELDS:
        if field == "国家":
            mapping[field] = "国家" if has_cn_country else ""
            continue

        best = ""
        best_key = (-10**9, -10**9, -10**9)  # (score, -paren_flag, -len)
        for c in columns:
            if field == "重量KG":
                if not strict_weightkg(c):
                    continue
                score = 100 + keyword_rank(c, field)
            else:
                score = keyword_rank(c, field)

            # 精确列名轻微加分（更像“就叫这个”）
            if normalize_spaces(c) == field:
                score += 20

            n = safe_str(c).lower()
            paren = 1 if ("(" in n or "（" in n or ")" in n or "）" in n) else 0
            # 同分时：优先无括号/后缀，再优先更短列名（例如 importer 优先于 importer(VN)）
            key = (score, -paren, -len(safe_str(c)))

            if key > best_key:
                best_key = key
                best = c if score > 0 else ""
        mapping[field] = best
    return mapping


def candidate_columns_for_field(field: str, columns: List[str], chosen: str = "", max_items: int = MAX_CANDIDATES) -> List[str]:
    """
    下拉候选列：按关键词得分排序，只保留得分>0的列（完全无关的不显示）。
    但如果已有 chosen（来自记忆/用户之前选择），即使得分为0，也会强制加入，避免“记忆映射丢失”。
    """
    scored: List[Tuple[int, str]] = []
    for c in columns:
        sc = 0
        if field == "重量KG":
            if not strict_weightkg(c):
                continue
            sc = 100 + keyword_rank(c, field)
        elif field == "国家":
            # “国家”精确命中最高优先
            if normalize_spaces(c) == "国家":
                sc = 999
            else:
                sc = keyword_rank(c, field)
        else:
            sc = keyword_rank(c, field)
            if normalize_spaces(c) == field:
                sc += 20

        if sc > 0:
            scored.append((sc, c))

    scored.sort(key=lambda x: (-x[0], len(x[1])))

    out = [c for _, c in scored[:max_items]]

    if chosen and chosen in columns and chosen not in out:
        out.append(chosen)
    return out



def ensure_dir_writable(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)
    test = p / ".write_test.tmp"
    test.write_text("ok", encoding="utf-8")
    test.unlink(missing_ok=True)


def load_template_columns(template_path: Optional[Path]) -> List[str]:
    if not template_path:
        return DEFAULT_OUTPUT_COLUMNS.copy()
    raw = pd.read_excel(template_path, sheet_name=0, header=None, engine="openpyxl")
    if raw.empty:
        return DEFAULT_OUTPUT_COLUMNS.copy()
    first_row = raw.iloc[0].tolist()
    cols = [normalize_spaces(safe_str(v)) for v in first_row if normalize_spaces(safe_str(v))]
    return cols if cols else DEFAULT_OUTPUT_COLUMNS.copy()


# --------------------------- 映射记忆存储（按表结构） ---------------------------

class MappingProfileStore:
    """映射方案存储：按“表结构 fingerprint”保存多个可命名方案。

    - 支持：保存/另存为/重命名/删除/设置最后使用
    - 自动迁移旧版 mappings.json（单映射）到 profiles.json（默认方案）
    """
    def __init__(self, store_dir: Path):
        self.store_dir = store_dir
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self.path = store_dir / "profiles.json"
        self.legacy_path = store_dir / "mappings.json"
        self.data: Dict[str, Dict[str, Any]] = {}
        self._load()
        self._maybe_migrate_legacy()

    def _load(self) -> None:
        try:
            if self.path.exists():
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
            else:
                self.data = {}
        except Exception:
            self.data = {}

    def save(self) -> None:
        try:
            self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _now(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")

    def _maybe_migrate_legacy(self) -> None:
        """把旧版 {fingerprint:{mapping:{...}}} 迁移成 profiles.json。"""
        try:
            if not self.legacy_path.exists():
                return
            legacy = json.loads(self.legacy_path.read_text(encoding="utf-8"))
            if not isinstance(legacy, dict) or not legacy:
                return
            changed = False
            for fp, obj in legacy.items():
                if fp in self.data:
                    continue
                mapping = None
                if isinstance(obj, dict):
                    mapping = obj.get("mapping")
                if not isinstance(mapping, dict):
                    continue
                self.data[fp] = {
                    "profiles": [
                        {
                            "name": "默认方案",
                            "mapping": {k: (v or "") for k, v in mapping.items()},
                            "updated_at": obj.get("updated_at", self._now()),
                        }
                    ],
                    "last_used": "默认方案",
                }
                changed = True
            if changed:
                self.save()
        except Exception:
            return

    def list_profiles(self, fingerprint: str) -> List[str]:
        obj = self.data.get(fingerprint, {})
        profs = obj.get("profiles", [])
        out = []
        if isinstance(profs, list):
            for p in profs:
                if isinstance(p, dict) and safe_str(p.get("name")):
                    out.append(safe_str(p.get("name")))
        return out

    def get_last_used(self, fingerprint: str) -> Optional[str]:
        obj = self.data.get(fingerprint, {})
        lu = obj.get("last_used")
        return safe_str(lu) if safe_str(lu) else None

    def set_last_used(self, fingerprint: str, name: str) -> None:
        if fingerprint not in self.data:
            self.data[fingerprint] = {"profiles": [], "last_used": name}
        else:
            self.data[fingerprint]["last_used"] = name
        self.save()

    def get_profile(self, fingerprint: str, name: str) -> Optional[Dict[str, str]]:
        obj = self.data.get(fingerprint, {})
        profs = obj.get("profiles", [])
        if not isinstance(profs, list):
            return None
        for p in profs:
            if isinstance(p, dict) and safe_str(p.get("name")) == name:
                mapping = p.get("mapping", {})
                if isinstance(mapping, dict):
                    return {k: (v or "") for k, v in mapping.items()}
        return None

    def save_profile(self, fingerprint: str, name: str, mapping: Dict[str, str]) -> None:
        name = safe_str(name)
        if not name:
            return
        obj = self.data.setdefault(fingerprint, {"profiles": [], "last_used": name})
        profs = obj.get("profiles", [])
        if not isinstance(profs, list):
            profs = []
            obj["profiles"] = profs
        # 覆盖同名
        for p in profs:
            if isinstance(p, dict) and safe_str(p.get("name")) == name:
                p["mapping"] = {k: (v or "") for k, v in mapping.items()}
                p["updated_at"] = self._now()
                obj["last_used"] = name
                self.save()
                return
        profs.append({"name": name, "mapping": {k: (v or "") for k, v in mapping.items()}, "updated_at": self._now()})
        obj["last_used"] = name
        self.save()

    def rename_profile(self, fingerprint: str, old: str, new_name: str) -> bool:
        old = safe_str(old)
        new_name = safe_str(new_name)
        if not old or not new_name or old == new_name:
            return False
        obj = self.data.get(fingerprint, {})
        profs = obj.get("profiles", [])
        if not isinstance(profs, list):
            return False
        if new_name in self.list_profiles(fingerprint):
            return False
        for p in profs:
            if isinstance(p, dict) and safe_str(p.get("name")) == old:
                p["name"] = new_name
                p["updated_at"] = self._now()
                if obj.get("last_used") == old:
                    obj["last_used"] = new_name
                self.save()
                return True
        return False

    def delete_profile(self, fingerprint: str, name: str) -> bool:
        name = safe_str(name)
        obj = self.data.get(fingerprint, {})
        profs = obj.get("profiles", [])
        if not isinstance(profs, list):
            return False
        before = len(profs)
        profs2 = [p for p in profs if not (isinstance(p, dict) and safe_str(p.get("name")) == name)]
        if len(profs2) == before:
            return False
        obj["profiles"] = profs2
        # 修正 last_used
        if obj.get("last_used") == name:
            obj["last_used"] = profs2[0]["name"] if profs2 else ""
        self.save()
        return True

class ProfileManagerDialog(QDialog):
    """管理当前结构组的映射方案：重命名/删除/设为默认(最后使用)。"""
    def __init__(self, parent: QWidget, store: MappingProfileStore, fingerprint: str, current: str):
        super().__init__(parent)
        self.store = store
        self.fingerprint = fingerprint
        self.current = current
        self.setWindowTitle("管理映射方案")
        self.resize(520, 360)
        self._build_ui()
        self._refresh()

    def _build_ui(self):
        root = QVBoxLayout(self)
        tip = QLabel("提示：方案是按“表结构”保存的。不同结构组互不影响。")
        tip.setWordWrap(True)
        root.addWidget(tip)

        self.list = QListWidget()
        root.addWidget(self.list, 1)

        btns = QHBoxLayout()
        root.addLayout(btns)

        self.btn_rename = QPushButton("重命名")
        self.btn_delete = QPushButton("删除")
        self.btn_set_default = QPushButton("设为默认")
        btns.addWidget(self.btn_rename)
        btns.addWidget(self.btn_delete)
        btns.addWidget(self.btn_set_default)
        btns.addStretch(1)

        okbar = QHBoxLayout()
        root.addLayout(okbar)
        okbar.addStretch(1)
        self.btn_close = QPushButton("关闭")
        okbar.addWidget(self.btn_close)

        self.btn_close.clicked.connect(self.accept)
        self.btn_rename.clicked.connect(self._rename)
        self.btn_delete.clicked.connect(self._delete)
        self.btn_set_default.clicked.connect(self._set_default)

    def _refresh(self):
        self.list.clear()
        names = self.store.list_profiles(self.fingerprint)
        for n in names:
            item = QListWidgetItem(n)
            if n == self.store.get_last_used(self.fingerprint):
                item.setText(f"{n}  （默认）")
                item.setData(Qt.UserRole, n)
                f = item.font()
                f.setBold(True)
                item.setFont(f)
            else:
                item.setData(Qt.UserRole, n)
            self.list.addItem(item)

        # 选中当前
        for i in range(self.list.count()):
            it = self.list.item(i)
            if it.data(Qt.UserRole) == self.current:
                self.list.setCurrentRow(i)
                break

    def _selected_name(self) -> Optional[str]:
        it = self.list.currentItem()
        return it.data(Qt.UserRole) if it else None

    def _rename(self):
        old = self._selected_name()
        if not old:
            return
        new_name, ok = QInputDialog.getText(self, "重命名方案", "新名称：", text=old)
        if not ok:
            return
        new_name = safe_str(new_name)
        if not new_name:
            return
        if not self.store.rename_profile(self.fingerprint, old, new_name):
            QMessageBox.warning(self, "失败", "重命名失败：名称重复或无效。")
            return
        self.current = new_name
        self._refresh()

    def _delete(self):
        name = self._selected_name()
        if not name:
            return
        if QMessageBox.question(self, "确认删除", f"确定删除方案：{name} ？") != QMessageBox.Yes:
            return
        self.store.delete_profile(self.fingerprint, name)
        # 若删掉当前，自动切到 last_used
        self.current = self.store.get_last_used(self.fingerprint) or ""
        self._refresh()

    def _set_default(self):
        name = self._selected_name()
        if not name:
            return
        self.store.set_last_used(self.fingerprint, name)
        self.current = name
        self._refresh()

    def get_current(self) -> str:
        return self.current

# --------------------------- 列配置（全局 UI 记忆） ---------------------------

def default_column_configs(output_cols: List[str]) -> List[Dict[str, Any]]:
    cfgs: List[Dict[str, Any]] = []
    for c in output_cols:
        mode = MODE_AUTO if c in AUTO_MAP_FIELDS else MODE_EMPTY
        default_val = ""

        # 固定默认
        if c == "负责人":
            mode = MODE_DEFAULT
            default_val = RESPONSIBLE_NAME

        # 国家：默认自动映射（若没有精确“国家”列，建议映射会是“无”，用户可切换手动/留空）
        if c == "国家":
            mode = MODE_AUTO
            default_val = ""

        # 合作供应商：默认显示“无”（但允许用户在“映射框设置”中切换为自动映射/手动输入）
        if c in ["合作供应商1", "合作供应商2", "合作供应商3"]:
            mode = MODE_DEFAULT
            default_val = "无"

        # 数据来源：默认自动（特殊：即便不映射也会使用文件名来源作为保底）
        if c == "数据来源":
            mode = MODE_AUTO
            default_val = ""

        width = 180
        if c in ["公司简介", "HSCODE商品描述", "产品明细"]:
            width = 280
        elif c in ["客户名称"]:
            width = 220
        elif c in ["数据来源"]:
            width = 220

        cfgs.append({
            "name": c,
            "enabled": True,
            "width": width,
            "mode": mode,
            "default": default_val,
        })
    return cfgs


def merge_column_configs(output_cols: List[str], saved_cfgs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_name = {c.get("name"): c for c in saved_cfgs if isinstance(c, dict) and c.get("name")}
    out = []
    defaults = {c["name"]: c for c in default_column_configs(output_cols)}
    for name in output_cols:
        base = defaults.get(name, {"name": name, "enabled": True, "width": 180, "mode": MODE_EMPTY, "default": ""})
        old = by_name.get(name)
        if old:
            merged = base.copy()
            merged.update({
                "enabled": bool(old.get("enabled", base["enabled"])),
                "width": int(old.get("width", base["width"])),
                "mode": old.get("mode", base["mode"]),
                "default": old.get("default", base["default"]),
            })
            if name == "负责人":
                merged["mode"] = MODE_DEFAULT
                merged["default"] = RESPONSIBLE_NAME
            out.append(merged)
        else:
            out.append(base)
    return out


# --------------------------- “映射框设置”对话框 ---------------------------

class ColumnSettingsDialog(QDialog):
    def __init__(self, parent: QWidget, column_cfgs: List[Dict[str, Any]], keywords: Dict[str, List[Tuple[str, int]]]):
        super().__init__(parent)
        self.setWindowTitle("映射框设置")
        self.resize(900, 520)
        self._cfgs = [c.copy() for c in column_cfgs]
        self._keywords = json.loads(json.dumps(keywords, ensure_ascii=False))
        self._build_ui()
        self._load_to_table()

    def _build_ui(self):
        root = QVBoxLayout(self)

        info = QLabel("按列顺序配置：列宽/启用禁用/删除/添加；并设置模式与默认值（手动输入列的初始值也在这里设）。系统会记住。")
        info.setWordWrap(True)
        root.addWidget(info)

        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["顺序", "列名", "启用", "宽度(px)", "模式", "默认值/手动输入初始值"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(5, QHeaderView.Stretch)
        self.table.setAlternatingRowColors(True)
        root.addWidget(self.table, 1)

        btn_bar = QHBoxLayout()
        root.addLayout(btn_bar)

        self.btn_add = QPushButton("添加列")
        self.btn_del = QPushButton("删除选中列")
        self.btn_reset = QPushButton("恢复默认")
        self.btn_keywords = QPushButton("关键字设置")
        btn_bar.addWidget(self.btn_add)
        btn_bar.addWidget(self.btn_del)
        btn_bar.addStretch(1)
        btn_bar.addWidget(self.btn_keywords)
        btn_bar.addWidget(self.btn_reset)

        self.btn_add.clicked.connect(self._add_row)
        self.btn_del.clicked.connect(self._delete_selected)
        self.btn_reset.clicked.connect(self._reset_default)
        self.btn_keywords.clicked.connect(self._open_keyword_settings)

        ok_bar = QHBoxLayout()
        root.addLayout(ok_bar)
        ok_bar.addStretch(1)
        self.btn_ok = QPushButton("保存")
        self.btn_cancel = QPushButton("取消")
        ok_bar.addWidget(self.btn_ok)
        ok_bar.addWidget(self.btn_cancel)
        self.btn_ok.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)

    def _load_to_table(self):
        self.table.setRowCount(len(self._cfgs))
        for i, cfg in enumerate(self._cfgs):
            item_idx = QTableWidgetItem(str(i + 1))
            item_idx.setFlags(item_idx.flags() & ~Qt.ItemIsEditable)
            self.table.setItem(i, 0, item_idx)

            item_name = QTableWidgetItem(cfg.get("name", ""))
            self.table.setItem(i, 1, item_name)

            chk = QCheckBox()
            chk.setChecked(bool(cfg.get("enabled", True)))
            chk.setStyleSheet("QCheckBox{padding-left:6px;}")
            self.table.setCellWidget(i, 2, chk)

            item_w = QTableWidgetItem(str(int(cfg.get("width", 180))))
            self.table.setItem(i, 3, item_w)

            cmb = QComboBox()
            cmb.addItems(MODE_CHOICES)
            mode = cfg.get("mode", MODE_EMPTY)
            if mode not in MODE_CHOICES:
                mode = MODE_EMPTY
            cmb.setCurrentText(mode)
            self.table.setCellWidget(i, 4, cmb)

            item_def = QTableWidgetItem(str(cfg.get("default", "")))
            self.table.setItem(i, 5, item_def)

        self.table.resizeRowsToContents()

    def _collect(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for i in range(self.table.rowCount()):
            name = safe_str(self.table.item(i, 1).text() if self.table.item(i, 1) else "")
            if not name:
                continue
            chk = self.table.cellWidget(i, 2)
            enabled = chk.isChecked() if isinstance(chk, QCheckBox) else True
            try:
                width = int(safe_str(self.table.item(i, 3).text() if self.table.item(i, 3) else "180") or 180)
            except Exception:
                width = 180
            cmb = self.table.cellWidget(i, 4)
            mode = cmb.currentText() if isinstance(cmb, QComboBox) else MODE_EMPTY
            default_val = safe_str(self.table.item(i, 5).text() if self.table.item(i, 5) else "")

            if name == "负责人":
                enabled = True
                mode = MODE_DEFAULT
                default_val = RESPONSIBLE_NAME

            out.append({"name": name, "enabled": enabled, "width": width, "mode": mode, "default": default_val})
        return out

    def _add_row(self):
        r = self.table.rowCount()
        self.table.insertRow(r)
        it = QTableWidgetItem(str(r + 1))
        it.setFlags(it.flags() & ~Qt.ItemIsEditable)
        self.table.setItem(r, 0, it)
        self.table.setItem(r, 1, QTableWidgetItem("新列名"))
        chk = QCheckBox(); chk.setChecked(True)
        self.table.setCellWidget(r, 2, chk)
        self.table.setItem(r, 3, QTableWidgetItem("180"))
        cmb = QComboBox(); cmb.addItems(MODE_CHOICES); cmb.setCurrentText(MODE_EMPTY)
        self.table.setCellWidget(r, 4, cmb)
        self.table.setItem(r, 5, QTableWidgetItem(""))
        self.table.scrollToBottom()

    def _delete_selected(self):
        rows = sorted({i.row() for i in self.table.selectedIndexes()}, reverse=True)
        for r in rows:
            self.table.removeRow(r)
        for i in range(self.table.rowCount()):
            it = self.table.item(i, 0)
            if it:
                it.setText(str(i + 1))

    def _reset_default(self):
        names = []
        for i in range(self.table.rowCount()):
            nm = safe_str(self.table.item(i, 1).text() if self.table.item(i, 1) else "")
            if nm:
                names.append(nm)
        self._cfgs = default_column_configs(names)
        self._load_to_table()


    def _copy_as_json(self):
        try:
            cfgs = self._collect()
            txt = json.dumps(cfgs, ensure_ascii=False, indent=2)
            QApplication.clipboard().setText(txt)
            QMessageBox.information(self, "已复制", "已将当前配置复制到剪贴板（JSON）。")
        except Exception as e:
            QMessageBox.warning(self, "复制失败", f"复制失败：{e}")

    def _open_keyword_settings(self):
        dlg = KeywordSettingsDialog(self, self._keywords)
        if dlg.exec() == QDialog.Accepted:
            self._keywords = dlg.get_keywords()

    def get_keywords(self) -> Dict[str, List[Tuple[str, int]]]:
        return _normalize_keywords_dict(self._keywords)

    def get_result(self) -> List[Dict[str, Any]]:
        return self._collect()

# --------------------------- 工作线程 ---------------------------


# --------------------------- “关键字设置”对话框 ---------------------------

class KeywordSettingsDialog(QDialog):
    """维护 KEYWORDS（支持负权重）。

    为了更直观：按“字段”分组编辑。
    - 左侧：字段下拉
    - 右侧：该字段的关键词/权重/启用
    """
    def __init__(self, parent: QWidget, keywords: Dict[str, List[Tuple[str, int]]]):
        super().__init__(parent)
        self.setWindowTitle("关键字设置（自动映射推荐）")
        self.resize(860, 560)

        # 深拷贝，避免取消时污染
        base = json.loads(json.dumps(keywords, ensure_ascii=False))
        self._kw = _normalize_keywords_dict(base)

        # 启用状态：默认全部启用（用户可临时禁用）
        # 结构：{field:[{"kw":str,"w":int,"on":bool},...]}
        self._rules: Dict[str, List[Dict[str, Any]]] = {}
        for f, lst in self._kw.items():
            self._rules[f] = [{"kw": k, "w": int(w), "on": True} for k, w in lst]

        self._build_ui()
        self._load_fields()

    def _all_fields(self) -> List[str]:
        fields = list(DEFAULT_KEYWORDS.keys())
        for f in AUTO_MAP_FIELDS:
            if f not in fields:
                fields.append(f)
        # 也加入输出列中可能出现的新字段
        for f in DEFAULT_OUTPUT_COLUMNS:
            if f not in fields:
                fields.append(f)
        return fields

    def _build_ui(self):
        root = QVBoxLayout(self)

        tip = QLabel(
            "说明：每个字段会对原始列名进行打分（命中关键字权重相加）。\n"
            "下拉候选仅展示 score>0 的列；负权重用于排除无关（例如 address/phone/id）。"
        )
        tip.setWordWrap(True)
        root.addWidget(tip)

        top = QHBoxLayout()
        root.addLayout(top)

        top.addWidget(QLabel("字段："))
        self.field_sel = QComboBox()
        self.field_sel.currentIndexChanged.connect(self._on_field_changed)
        top.addWidget(self.field_sel, 2)

        self.btn_add = QPushButton("添加规则")
        self.btn_del = QPushButton("删除选中")
        self.btn_reset = QPushButton("恢复默认")
        top.addWidget(self.btn_add, 0)
        top.addWidget(self.btn_del, 0)
        top.addWidget(self.btn_reset, 0)
        top.addStretch(1)

        self.table = QTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(["关键词", "权重", "启用"])
        self.table.horizontalHeader().setStretchLastSection(False)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.DoubleClicked | QAbstractItemView.EditKeyPressed | QAbstractItemView.SelectedClicked)
        self.table.setWordWrap(False)
        root.addWidget(self.table, 1)

        okbar = QHBoxLayout()
        root.addLayout(okbar)
        okbar.addStretch(1)
        self.btn_ok = QPushButton("保存")
        self.btn_cancel = QPushButton("取消")
        okbar.addWidget(self.btn_ok)
        okbar.addWidget(self.btn_cancel)

        self.btn_ok.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)
        self.btn_add.clicked.connect(self._add_row)
        self.btn_del.clicked.connect(self._delete_selected)
        self.btn_reset.clicked.connect(self._reset_defaults)

        self._current_field = ""

    def _open_keyword_settings(self):
        QMessageBox.information(self, "提示", "你已经在关键字设置界面，无需再次打开。")

    def _load_fields(self):
        fields = self._all_fields()
        self.field_sel.blockSignals(True)
        self.field_sel.clear()
        self.field_sel.addItems(fields)
        self.field_sel.blockSignals(False)
        # 默认选中“客户名称”
        idx = self.field_sel.findText("客户名称")
        self.field_sel.setCurrentIndex(idx if idx >= 0 else 0)
        self._on_field_changed()

    def _save_current_table(self):
        field = self._current_field
        if not field:
            return
        rules: List[Dict[str, Any]] = []
        for r in range(self.table.rowCount()):
            kw_item = self.table.item(r, 0)
            w_item = self.table.item(r, 1)
            kw = safe_str(kw_item.text() if kw_item else "")
            if not kw:
                continue
            try:
                w = int(safe_str(w_item.text() if w_item else "0") or 0)
            except Exception:
                w = 0
            chk = self.table.cellWidget(r, 2)
            on = True
            if isinstance(chk, QCheckBox):
                on = chk.isChecked()
            elif isinstance(chk, QWidget):
                cb = chk.findChild(QCheckBox)
                if cb is not None:
                    on = cb.isChecked()
            rules.append({"kw": kw, "w": w, "on": on})
        self._rules[field] = rules

    def _load_table_for(self, field: str):
        self.table.setRowCount(0)
        rules = self._rules.get(field, [])
        for rule in rules:
            r = self.table.rowCount()
            self.table.insertRow(r)
            self.table.setItem(r, 0, QTableWidgetItem(safe_str(rule.get("kw"))))
            self.table.setItem(r, 1, QTableWidgetItem(str(int(rule.get("w", 0)))))
            chk = QCheckBox()
            chk.setChecked(bool(rule.get("on", True)))
            w = QWidget()
            lay = QHBoxLayout(w)
            lay.setContentsMargins(0, 0, 0, 0)
            lay.addStretch(1)
            lay.addWidget(chk)
            lay.addStretch(1)
            self.table.setCellWidget(r, 2, w)

    def _on_field_changed(self):
        # 切换字段时，先保存当前表格内容
        self._save_current_table()
        field = safe_str(self.field_sel.currentText())
        self._current_field = field
        self._load_table_for(field)

    def _add_row(self):
        r = self.table.rowCount()
        self.table.insertRow(r)
        self.table.setItem(r, 0, QTableWidgetItem(""))
        self.table.setItem(r, 1, QTableWidgetItem("1"))
        chk = QCheckBox()
        chk.setChecked(True)
        w = QWidget()
        lay = QHBoxLayout(w)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.addStretch(1)
        lay.addWidget(chk)
        lay.addStretch(1)
        self.table.setCellWidget(r, 2, w)
        self.table.setCurrentCell(r, 0)
        self.table.editItem(self.table.item(r, 0))

    def _delete_selected(self):
        rows = sorted({idx.row() for idx in self.table.selectedIndexes()}, reverse=True)
        for r in rows:
            self.table.removeRow(r)

    def _reset_defaults(self):
        if QMessageBox.question(self, "恢复默认", "将关键字恢复为内置默认值？（你自定义的会被覆盖）") != QMessageBox.Yes:
            return
        self._rules = {f: [{"kw": k, "w": int(w), "on": True} for k, w in lst] for f, lst in DEFAULT_KEYWORDS.items()}
        self._on_field_changed()


    def _copy_as_code(self):
        # 先保存当前页
        self._save_current_table()
        kws = self.get_keywords()
        lines = []
        lines.append("KEYWORDS = {")
        for field, lst in kws.items():
            lines.append(f'    "{field}": [')
            for kw, w in lst:
                lines.append(f'        ("{kw}", {int(w)}),')
            lines.append("    ],")
        lines.append("}")
        text = "\n".join(lines)
        QApplication.clipboard().setText(text)
        QMessageBox.information(self, "已复制", "已复制到剪贴板（可直接粘贴到代码里）。")


    def get_keywords(self) -> Dict[str, List[Tuple[str, int]]]:
        # 保存当前页
        self._save_current_table()
        out: Dict[str, List[Tuple[str, int]]] = {}
        for field, rules in self._rules.items():
            lst: List[Tuple[str, int]] = []
            for rule in rules:
                if not rule.get("on", True):
                    continue
                kw = safe_str(rule.get("kw"))
                if not kw:
                    continue
                try:
                    w = int(rule.get("w", 0))
                except Exception:
                    w = 0
                lst.append((kw, w))
            if lst:
                out[field] = lst
        return _normalize_keywords_dict(out)


class Worker(QObject):
    progress = Signal(int)
    step = Signal(str)
    log = Signal(str)
    finished = Signal(bool, str)
    output_ready = Signal(str)
    output_file_ready = Signal(str)

    def __init__(
        self,
        groups: List[GroupAnalysis],
        group_mappings: Dict[str, Dict[str, str]],
        output_columns: List[str],
        column_cfgs: List[Dict[str, Any]],
        manual_values: Dict[str, str],
        output_dir: Path,
        merge_output: bool,
        parent: Optional[QObject] = None,
    ):
        super().__init__(parent)
        self.groups = groups
        self.group_mappings = group_mappings
        self.output_columns = output_columns
        self.column_cfgs = column_cfgs
        self.manual_values = manual_values
        self.output_dir = output_dir
        self.merge_output = merge_output
        self.cfg_by_name = {c["name"]: c for c in column_cfgs if c.get("name")}

    def run(self) -> None:
        try:
            ensure_dir_writable(self.output_dir)
        except Exception as e:
            self.finished.emit(False, f"输出目录不可写：{self.output_dir}\n{e}")
            return

        try:
            all_rows: List[pd.DataFrame] = []
            total_files = sum(len(g.files) for g in self.groups)
            done_files = 0

            def log_line(msg: str) -> None:
                self.log.emit(msg)

            log_line(f"[INFO] 输出目录：{self.output_dir}")
            ts = time.strftime("%Y%m%d_%H%M%S")

            for group in self.groups:
                mapping = self.group_mappings.get(group.fingerprint, {})
                if not mapping.get(REQUIRED_AUTO_MAP_FIELD):
                    raise ValueError(f"结构组 {group.fingerprint[:8]} 缺少必需映射：{REQUIRED_AUTO_MAP_FIELD}")

                for fa in group.files:
                    done_files += 1
                    pct = int(done_files * 100 / max(total_files, 1))
                    self.progress.emit(min(pct, 92))
                    self.step.emit("读取与清洗原始表格")
                    log_line(f"[FILE] 处理：{fa.path.name}（header_row={fa.header_row}）")

                    raw = read_raw_table(fa.path)
                    df, cols = build_table_from_raw(raw, fa.header_row)

                    df = df.dropna(how="all")
                    df = df[~df.apply(looks_like_footer, axis=1)]
                    df = df.replace(r"^\s*$", pd.NA, regex=True)
                    try:
                        df = df.infer_objects(copy=False)
                    except Exception:
                        pass
                    df = df.dropna(how="all")
                    if df.empty:
                        log_line("[WARN] 该文件有效数据为空，跳过。")
                        continue

                    self.step.emit("抽取字段并标准化")
                    extracted = self._extract_rows(df, mapping, log_line, source_name=file_base_name(fa.path))
                    if extracted.empty:
                        log_line("[WARN] 抽取结果为空（可能客户名称列全空），跳过。")
                        continue

                    if self.merge_output:
                        all_rows.append(extracted)
                    else:
                        self.step.emit("聚合去重并写入输出")
                        final = self._aggregate(extracted)
                        out_file = self.output_dir / f"处理结果_{fa.path.stem}_{ts}.xlsx"
                        self._write_output(final, out_file)
                        self.output_file_ready.emit(str(out_file))
                        log_line(f"[OK] 已输出：{out_file.name}")

            if self.merge_output:
                self.step.emit("合并、聚合去重并写入输出")
                merged = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
                final = self._aggregate(merged)
                out_file = self.output_dir / f"处理结果_合并_{ts}.xlsx"
                self._write_output(final, out_file)
                self.output_file_ready.emit(str(out_file))
                log_line(f"[OK] 已输出：{out_file.name}")

            self.progress.emit(100)
            self.output_ready.emit(str(self.output_dir))
            self.finished.emit(True, f"任务已完成。输出目录：{self.output_dir}")
        except Exception as e:
            tb = traceback.format_exc()
            self.finished.emit(False, f"处理失败：{e}\n\n{tb}")

    def _extract_rows(self, df: pd.DataFrame, mapping: Dict[str, str], log_line, source_name: str = "") -> pd.DataFrame:
        def get_col(field: str) -> Optional[str]:
            c = mapping.get(field, "")
            return c if c and c in df.columns else None

        col_company = get_col("客户名称")
        if not col_company:
            return pd.DataFrame()

        col_date = get_col("最近货运日期")
        col_weight = get_col("重量KG")
        col_ship = get_col("海关提单数")
        col_prod = get_col("产品明细")
        col_hsdesc = get_col("HSCODE商品描述")
        col_country = get_col("国家")
        col_amount = get_col("交易总额")
        col_website = get_col("公司官网")
        col_intro = get_col("公司简介")

        col_sup1 = get_col("合作供应商1")
        col_sup2 = get_col("合作供应商2")
        col_sup3 = get_col("合作供应商3")
        col_source = get_col("数据来源")

        out = pd.DataFrame()
        out["客户名称_raw"] = df[col_company].map(safe_str)
        out["客户名称"] = out["客户名称_raw"].map(normalize_spaces)
        out["客户名称_key"] = out["客户名称_raw"].map(company_norm_key)

        out["_date"] = parse_date_series(df[col_date]) if col_date else pd.NaT
        out["_weight"] = parse_number_series(df[col_weight]) if col_weight else pd.NA

        if col_ship:
            out["_shipments"] = parse_number_series(df[col_ship])
            out["_ship_fallback"] = 0
        else:
            out["_shipments"] = 1
            out["_ship_fallback"] = 1

        out["_prod"] = df[col_prod].map(safe_str) if col_prod else ""
        out["_hsdesc"] = df[col_hsdesc].map(safe_str) if col_hsdesc else ""
        out["_country"] = df[col_country].map(safe_str) if col_country else ""
        out["_amount"] = parse_number_series(df[col_amount]) if col_amount else pd.NA
        out["_website"] = df[col_website].map(safe_str) if col_website else ""
        out["_intro"] = df[col_intro].map(safe_str) if col_intro else ""

        out["_sup1"] = df[col_sup1].map(safe_str) if col_sup1 else ""
        out["_sup2"] = df[col_sup2].map(safe_str) if col_sup2 else ""
        out["_sup3"] = df[col_sup3].map(safe_str) if col_sup3 else ""
        out["_source_col"] = df[col_source].map(safe_str) if col_source else ""
        out["_source_file"] = source_name or ""
        # 行级数据来源：表内来源 + 文件名来源（去重）
        out["_source"] = out.apply(lambda r: dedup_join([safe_str(r.get("_source_col", "")), safe_str(r.get("_source_file", ""))], "|"), axis=1)
        before = len(out)
        out = out[out["客户名称_key"].map(lambda x: bool(str(x).strip()))]
        after = len(out)
        if after < before:
            log_line(f"[INFO] 过滤客户名称为空行：{before - after} 行")

        return out

    def _aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return self._build_output(pd.DataFrame())

        def agg_group(g: pd.DataFrame) -> pd.Series:
            name = next((x for x in g["客户名称"].tolist() if x), "")
            dmax = g["_date"].max()
            dstr = "" if pd.isna(dmax) else pd.to_datetime(dmax).strftime("%Y-%m-%d")

            wsum = g["_weight"].sum(min_count=1)
            asum = g["_amount"].sum(min_count=1)
            # 海关提单数：
            # - 若源表未提供该列（或映射选择“无”），按同公司出现的行数计算；
            # - 若源表提供了该列，则取“该列最大值”与“行数”两者中的较大者（避免被重复行数覆盖）。
            row_cnt = len(g)
            if "_ship_fallback" in g.columns and (g["_ship_fallback"] == 0).any():
                smax = g.loc[g["_ship_fallback"] == 0, "_shipments"].max(skipna=True)
                smax = 0 if pd.isna(smax) else smax
                s_final = max(float(smax), float(row_cnt))
            else:
                s_final = float(row_cnt)

            hs = dedup_join([safe_str(x) for x in g["_hsdesc"].tolist()], ";")
            prod = dedup_join([safe_str(x) for x in g["_prod"].tolist()], ";")
            country = dedup_join([safe_str(x) for x in g["_country"].tolist()], "|")

            website = next((normalize_spaces(safe_str(x)) for x in g["_website"].tolist() if safe_str(x)), "")
            intro = next((normalize_spaces(safe_str(x)) for x in g["_intro"].tolist() if safe_str(x)), "")

            ship_out = "" if pd.isna(s_final) else int(s_final) if float(s_final).is_integer() else s_final

            return pd.Series({
                "客户名称": name,
                "最近货运日期": dstr,
                "重量KG": "" if pd.isna(wsum) else wsum,
                "海关提单数": ship_out,
                "产品明细": prod,
                "HSCODE商品描述": hs,
                "国家": country,
                "公司官网": website,
                "公司简介": intro,
                "交易总额": "" if pd.isna(asum) else asum,
                "合作供应商1": dedup_join([safe_str(x) for x in g["_sup1"].tolist()], ";"),
                "合作供应商2": dedup_join([safe_str(x) for x in g["_sup2"].tolist()], ";"),
                "合作供应商3": dedup_join([safe_str(x) for x in g["_sup3"].tolist()], ";"),
                "数据来源": dedup_join([safe_str(x) for x in g["_source"].tolist()], "|"),
            })

        try:
            grouped = df.groupby("客户名称_key", dropna=False, sort=False).apply(agg_group, include_groups=False).reset_index(drop=True)
        except TypeError:
            grouped = df.groupby("客户名称_key", dropna=False, sort=False).apply(agg_group).reset_index(drop=True)
        return self._build_output(grouped)

    def _build_output(self, core: pd.DataFrame) -> pd.DataFrame:
        enabled_cols = [c for c in self.output_columns if self.cfg_by_name.get(c, {}).get("enabled", True)]
        n = len(core) if core is not None and not core.empty else 0
        out = pd.DataFrame(index=range(n), columns=enabled_cols).fillna("")

        if core is not None and not core.empty:
            for c in core.columns:
                if c in out.columns:
                    out[c] = core[c]

        for col in enabled_cols:
            cfg = self.cfg_by_name.get(col, {"mode": MODE_EMPTY, "default": ""})
            mode = cfg.get("mode", MODE_EMPTY)
            default_val = safe_str(cfg.get("default", ""))

            if mode == MODE_DEFAULT:
                out[col] = default_val
            elif mode == MODE_MANUAL:
                out[col] = safe_str(self.manual_values.get(col, default_val))
            elif mode == MODE_EMPTY:
                pass
            elif mode == MODE_AUTO:
                pass

        return out

    def _write_output(self, df: pd.DataFrame, out_file: Path) -> None:
        with pd.ExcelWriter(out_file, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="结果")


# --------------------------- GUI ---------------------------

class DropArea(QFrame):
    files_dropped = Signal(list)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.setObjectName("DropArea")
        layout = QVBoxLayout(self)
        self.label = QLabel("拖拽文件到这里\n或点击右侧“添加文件”")
        self.label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.label)

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dropEvent(self, event):
        urls = event.mimeData().urls()
        paths = []
        for u in urls:
            p = Path(u.toLocalFile())
            if p.is_file():
                paths.append(str(p))
        if paths:
            self.files_dropped.emit(paths)


class MainWindow(QMainWindow):
    VERSION = "v2.9.4"

    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"终极表格汇总处理程序 {self.VERSION}")
        self.resize(1280, 800)

        self.settings = QSettings("UltimateExcelProcessor", "UltimateExcelProcessorApp")
        # 载入用户自定义关键字（支持负权重）
        self.keywords = load_keywords_from_settings(self.settings)
        global KEYWORDS
        KEYWORDS = self.keywords
        store_dir = Path.home() / ".ultimate_excel_processor"
        self.mapping_store = MappingProfileStore(store_dir)
        self.current_profile_name: str = ""
        self._last_output_dir: str = ""
        self._last_output_file: str = ""

        self.file_analyses: List[FileAnalysis] = []
        self.groups: List[GroupAnalysis] = []
        self.group_mappings: Dict[str, Dict[str, str]] = {}
        self.current_group_fp: Optional[str] = None

        # 模板路径记忆：重启后仍使用上次选择的模板
        self.template_path_saved: str = safe_str(self.settings.value("template_path", ""))
        self.output_cols: List[str] = DEFAULT_OUTPUT_COLUMNS.copy()
        if self.template_path_saved and Path(self.template_path_saved).exists():
            try:
                self.output_cols = load_template_columns(Path(self.template_path_saved))
            except Exception:
                self.output_cols = DEFAULT_OUTPUT_COLUMNS.copy()
                self.template_path_saved = ""
        self.column_cfgs: List[Dict[str, Any]] = self._load_column_cfgs_for(self.output_cols)
        self.manual_values: Dict[str, str] = self._init_manual_values(self.column_cfgs)

        self._build_ui()
        self._build_menu()
        self._apply_column_widths()

    def _build_menu(self):
        # 菜单栏：把“使用说明”放到更符合习惯的位置
        mb = self.menuBar()
        help_menu = mb.addMenu("帮助")
        act_help = QAction("使用说明", self)
        act_help.triggered.connect(self._open_help)
        help_menu.addAction(act_help)

        act_about = QAction("关于", self)
        act_about.triggered.connect(lambda: QMessageBox.information(
            self,
            "关于",
            f"终极表格汇总处理程序 {self.VERSION}\nPySide6 + pandas/openpyxl"
        ))
        help_menu.addAction(act_about)

    def _load_column_cfgs_for(self, output_cols: List[str]) -> List[Dict[str, Any]]:
        raw = self.settings.value("column_cfgs_json", "")
        saved = []
        if raw:
            try:
                saved = json.loads(raw)
            except Exception:
                saved = []
        return merge_column_configs(output_cols, saved if isinstance(saved, list) else [])

    def _save_column_cfgs(self):
        self.settings.setValue("column_cfgs_json", json.dumps(self.column_cfgs, ensure_ascii=False))

    def _init_manual_values(self, cfgs: List[Dict[str, Any]]) -> Dict[str, str]:
        mv = {}
        for c in cfgs:
            if c.get("mode") == MODE_MANUAL:
                mv[c["name"]] = safe_str(c.get("default", ""))
        return mv

    def _cfg_by_name(self) -> Dict[str, Dict[str, Any]]:
        return {c["name"]: c for c in self.column_cfgs if c.get("name")}

    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        main = QVBoxLayout(root)
        main.setContentsMargins(12, 12, 12, 12)
        main.setSpacing(10)

        self.setStyleSheet("""
        QWidget { font-size: 12px; }
        QGroupBox { font-weight: 600; border: 1px solid #d0d0d0; border-radius: 10px; margin-top: 10px; }
        QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 6px; }
        #DropArea { border: 2px dashed #7a7a7a; border-radius: 12px; background: #fafafa; }
        QPushButton {
            background-color: #2f6fed; color: white; border: none;
            padding: 7px 12px; border-radius: 9px; font-weight: 600;
        }
        QPushButton:hover { background-color: #245bd6; }
        QPushButton:pressed { background-color: #1f4fbf; }
        QPushButton:disabled { background-color: #b7b7b7; color: #f3f3f3; }
        QLineEdit, QComboBox {
            border: 1px solid #cfcfcf; border-radius: 8px; padding: 6px;
            background: white;
        }
        QTextEdit { border: 1px solid #cfcfcf; border-radius: 10px; padding: 8px; background: white; }
        QProgressBar { border: 1px solid #cfcfcf; border-radius: 8px; text-align: center; height: 18px; }
        QProgressBar::chunk { background-color: #2f6fed; border-radius: 8px; }
        QTableWidget { border: 1px solid #cfcfcf; border-radius: 10px; background: white; gridline-color: #e6e6e6; }
        QHeaderView::section { background: #f4f4f4; padding: 6px; border: none; border-right: 1px solid #e1e1e1; font-weight: 700; }
        """)

        top = QHBoxLayout()
        main.addLayout(top, 2)

        self.drop_area = DropArea()
        self.drop_area.files_dropped.connect(self._add_files)
        top.addWidget(self.drop_area, 2)

        right = QVBoxLayout()
        top.addLayout(right, 1)

        btn_add = QPushButton("添加文件")
        btn_add.clicked.connect(self._pick_files)
        right.addWidget(btn_add)

        btn_clear = QPushButton("清空列表")
        btn_clear.clicked.connect(self._clear_files)
        right.addWidget(btn_clear)

        self.file_list = QListWidget()
        self.file_list.setSelectionMode(QListWidget.ExtendedSelection)
        right.addWidget(self.file_list, 1)

        btn_remove = QPushButton("移除选中")
        btn_remove.clicked.connect(self._remove_selected)
        right.addWidget(btn_remove)

        mid = QHBoxLayout()
        main.addLayout(mid, 1)

        box_left = QGroupBox("输出设置")
        mid.addWidget(box_left, 3)
        gl = QGridLayout(box_left)
        gl.setHorizontalSpacing(10)
        gl.setVerticalSpacing(8)

        gl.addWidget(QLabel("模板文件（可选）:"), 0, 0)
        self.template_path = QLineEdit()
        self.template_path.setPlaceholderText("不选则使用默认列")
        self.template_path.setReadOnly(True)
        if getattr(self, "template_path_saved", "") and Path(self.template_path_saved).exists():
            self.template_path.setText(self.template_path_saved)
        gl.addWidget(self.template_path, 0, 1)
        btn_tpl = QPushButton("选择模板")
        btn_tpl.clicked.connect(self._pick_template)
        gl.addWidget(btn_tpl, 0, 2)

        btn_tpl_clear = QPushButton("恢复默认")
        btn_tpl_clear.clicked.connect(self._clear_template)
        gl.addWidget(btn_tpl_clear, 0, 3)

        gl.addWidget(QLabel("输出目录:"), 1, 0)
        self.output_dir = QLineEdit()
        self.output_dir.setReadOnly(True)
        gl.addWidget(self.output_dir, 1, 1)
        btn_out = QPushButton("选择目录")
        btn_out.clicked.connect(self._pick_output_dir)
        gl.addWidget(btn_out, 1, 2)

        box_mode = QGroupBox("输出模式")
        mid.addWidget(box_mode, 1)
        vlm = QVBoxLayout(box_mode)
        self.rb_merge = QRadioButton("合并输出一个结果")
        self.rb_sep = QRadioButton("分别输出多个结果")
        self.rb_merge.setChecked(True)
        bg = QButtonGroup(self)
        bg.addButton(self.rb_merge)
        bg.addButton(self.rb_sep)
        vlm.addWidget(self.rb_merge)
        vlm.addWidget(self.rb_sep)
        vlm.addItem(QSpacerItem(20, 30, QSizePolicy.Minimum, QSizePolicy.Expanding))

        map_box = QGroupBox("列映射（按表结构分组）")
        main.addWidget(map_box, 4)
        map_layout = QVBoxLayout(map_box)

        bar = QHBoxLayout()
        map_layout.addLayout(bar)

        bar.addWidget(QLabel("结构组:"))
        self.group_selector = QComboBox()
        self.group_selector.currentIndexChanged.connect(self._on_group_changed)
        bar.addWidget(self.group_selector, 2)

        bar.addWidget(QLabel("方案:"))
        self.profile_selector = QComboBox()
        self.profile_selector.currentIndexChanged.connect(self._on_profile_changed)
        bar.addWidget(self.profile_selector, 1)

        self.btn_save_profile = QPushButton("保存方案")
        self.btn_save_profile.clicked.connect(self._save_profile)
        bar.addWidget(self.btn_save_profile, 0)

        self.btn_saveas_profile = QPushButton("另存为")
        self.btn_saveas_profile.clicked.connect(self._save_as_profile)
        bar.addWidget(self.btn_saveas_profile, 0)

        self.btn_manage_profiles = QPushButton("管理")
        self.btn_manage_profiles.clicked.connect(self._manage_profiles)
        bar.addWidget(self.btn_manage_profiles, 0)

        self.btn_mapping_settings = QPushButton("映射框设置")
        self.btn_mapping_settings.clicked.connect(self._open_mapping_settings)
        bar.addWidget(self.btn_mapping_settings, 0)

        self.group_info = QLabel("未分析文件")
        bar.addWidget(self.group_info, 3)

        self.map_table = QTableWidget()
        self.map_table.setRowCount(2)
        self.map_table.setVerticalHeaderLabels(["映射/默认", "样例（前5行）"])
        self.map_table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.map_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.map_table.setAlternatingRowColors(True)
        self.map_table.setWordWrap(False)
        self.map_table.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.map_table.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        map_layout.addWidget(self.map_table, 1)

        bottom = QHBoxLayout()
        main.addLayout(bottom, 2)

        left_run = QVBoxLayout()
        bottom.addLayout(left_run, 1)

        self.btn_analyze = QPushButton("分析文件并生成映射建议")
        self.btn_analyze.clicked.connect(self._analyze_files)
        left_run.addWidget(self.btn_analyze)

        self.btn_start = QPushButton("开始处理")
        self.btn_start.clicked.connect(self._start)
        left_run.addWidget(self.btn_start)

        self.btn_open_dir = QPushButton("打开输出目录")
        self.btn_open_dir.setEnabled(False)
        self.btn_open_dir.clicked.connect(self._open_output_dir)
        left_run.addWidget(self.btn_open_dir)

        self.step_label = QLabel("步骤：-")
        left_run.addWidget(self.step_label)

        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        left_run.addWidget(self.progress)

        self.status_label = QLabel("状态：就绪")
        left_run.addWidget(self.status_label)
        left_run.addItem(QSpacerItem(20, 40, QSizePolicy.Minimum, QSizePolicy.Expanding))

        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        bottom.addWidget(self.log_box, 2)

        last_out = self.settings.value("output_dir", "")
        if last_out and Path(last_out).exists():
            self.output_dir.setText(last_out)
        else:
            self.output_dir.setText(str(desktop_dir()))

        self._update_buttons()
        self._refresh_mapping_table_headers()

    def _update_buttons(self):
        has_files = self.file_list.count() > 0
        self.btn_analyze.setEnabled(has_files)
        self.btn_start.setEnabled(has_files and bool(self.groups))
        self.btn_save_profile.setEnabled(bool(self.groups))
        self.btn_saveas_profile.setEnabled(bool(self.groups))
        self.btn_manage_profiles.setEnabled(bool(self.groups))
        self.profile_selector.setEnabled(bool(self.groups))
        self.group_selector.setEnabled(bool(self.groups))

    def _log(self, msg: str):
        self.log_box.append(msg)
        self.log_box.ensureCursorVisible()

    def _refresh_mapping_table_headers(self):
        cfg_by = self._cfg_by_name()
        enabled_cols = [c for c in self.output_cols if cfg_by.get(c, {}).get("enabled", True)]
        self.map_table.setColumnCount(len(enabled_cols))
        self.map_table.setHorizontalHeaderLabels(enabled_cols)

        # 清空
        for r in range(self.map_table.rowCount()):
            for c in range(self.map_table.columnCount()):
                self.map_table.setCellWidget(r, c, None)
                self.map_table.setItem(r, c, None)

        # 第一行：对“手动/默认/留空”先渲染（自动映射列等 group 加载后再渲染下拉框）
        for ci, col in enumerate(enabled_cols):
            cfg = cfg_by.get(col, {"mode": MODE_EMPTY, "default": ""})
            mode = cfg.get("mode", MODE_EMPTY)
            default_val = safe_str(cfg.get("default", ""))

            if mode == MODE_MANUAL:
                le = QLineEdit()
                le.setPlaceholderText("手动输入")
                le.setText(safe_str(self.manual_values.get(col, default_val)))
                le.textChanged.connect(lambda txt, name=col: self._set_manual_value(name, txt))
                self.map_table.setCellWidget(0, ci, le)
            elif mode == MODE_DEFAULT:
                it = QTableWidgetItem(default_val)
                it.setFlags(it.flags() & ~Qt.ItemIsEditable)
                self.map_table.setItem(0, ci, it)
            elif mode == MODE_EMPTY:
                it = QTableWidgetItem("-")
                it.setFlags(it.flags() & ~Qt.ItemIsEditable)
                self.map_table.setItem(0, ci, it)
            elif mode == MODE_AUTO:
                # 自动映射列由 _render_mapping_row_for_group 填充下拉框
                pass

            # 样例行默认先填 "-"
            it2 = QTableWidgetItem("-")
            it2.setFlags(it2.flags() & ~Qt.ItemIsEditable)
            self.map_table.setItem(1, ci, it2)

        self._apply_column_widths()
        self.map_table.resizeRowsToContents()

    def _apply_column_widths(self):
        cfg_by = self._cfg_by_name()
        enabled_cols = [c for c in self.output_cols if cfg_by.get(c, {}).get("enabled", True)]
        for i, name in enumerate(enabled_cols):
            w = int(cfg_by.get(name, {}).get("width", 180))
            self.map_table.setColumnWidth(i, max(90, min(600, w)))

    def _set_manual_value(self, name: str, txt: str):
        self.manual_values[name] = txt

    def _pick_files(self):
        paths, _ = QFileDialog.getOpenFileNames(
            self, "选择原始表格文件", str(desktop_dir()), "Excel/CSV (*.xlsx *.xlsm *.xls *.csv)"
        )
        if paths:
            self._add_files(paths)

    def _add_files(self, paths: List[str]):
        existing = set()
        for i in range(self.file_list.count()):
            existing.add(self.file_list.item(i).data(Qt.UserRole))
        added = 0
        for p in paths:
            p = str(Path(p))
            if p in existing:
                continue
            ext = Path(p).suffix.lower()
            if ext not in [".xlsx", ".xlsm", ".xls", ".csv"]:
                continue
            item = QListWidgetItem(Path(p).name)
            item.setToolTip(p)
            item.setData(Qt.UserRole, p)
            self.file_list.addItem(item)
            added += 1

        if added:
            self._log(f"[INFO] 已添加 {added} 个文件。点击“分析文件并生成映射建议”。")
        self.groups = []
        self.group_selector.clear()
        self.group_info.setText("未分析文件")
        self._update_buttons()

    def _clear_files(self):
        self.file_list.clear()
        self.file_analyses = []
        self.groups = []
        self.group_selector.clear()
        self.group_info.setText("未分析文件")
        self._update_buttons()

    def _remove_selected(self):
        for item in self.file_list.selectedItems():
            row = self.file_list.row(item)
            self.file_list.takeItem(row)
        self.groups = []
        self.group_selector.clear()
        self.group_info.setText("未分析文件")
        self._update_buttons()

    def _pick_template(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "选择模板文件（xlsx）", str(desktop_dir()), "Excel (*.xlsx *.xlsm)"
        )
        if not path:
            return
        self.template_path.setText(path)
        self._log(f"[INFO] 已选择模板：{Path(path).name}")
        self.settings.setValue("template_path", path)

        try:
            self.output_cols = load_template_columns(Path(path))
        except Exception as e:
            QMessageBox.warning(self, "模板读取失败", f"读取模板失败，将继续使用默认列。\n{e}")
            self.output_cols = DEFAULT_OUTPUT_COLUMNS.copy()
            self.settings.setValue("template_path", "")
            self.template_path_saved = ""

        self.column_cfgs = self._load_column_cfgs_for(self.output_cols)
        self.manual_values = self._init_manual_values(self.column_cfgs)
        self._refresh_mapping_table_headers()
        if self.groups and self.group_selector.currentIndex() >= 0:
            self._on_group_changed(self.group_selector.currentIndex())


    def _clear_template(self):
        """清除已选择的模板，恢复为默认输出列。"""
        self.template_path.setText("")
        self.settings.setValue("template_path", "")
        self.template_path_saved = ""

        self.output_cols = DEFAULT_OUTPUT_COLUMNS.copy()
        self.column_cfgs = self._load_column_cfgs_for(self.output_cols)
        self.manual_values = self._init_manual_values(self.column_cfgs)
        self._refresh_mapping_table_headers()
        if self.groups and self.group_selector.currentIndex() >= 0:
            self._on_group_changed(self.group_selector.currentIndex())

        self._log("[INFO] 已恢复默认列（未使用模板）")

    def _pick_output_dir(self):
        path = QFileDialog.getExistingDirectory(self, "选择输出目录", self.output_dir.text() or str(desktop_dir()))
        if path:
            self.output_dir.setText(path)
            self.settings.setValue("output_dir", path)
            self._log(f"[INFO] 输出目录已设置：{path}")

    def _open_output_dir(self):
        p = self._get_output_dir()
        if p and p.exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(p)))

    def _get_output_dir(self) -> Optional[Path]:
        s = self.output_dir.text().strip()
        return Path(s) if s else None

    def _open_mapping_settings(self):
        dlg = ColumnSettingsDialog(self, self.column_cfgs, self.keywords)
        if dlg.exec() == QDialog.Accepted:
            cfgs = dlg.get_result()
            new_cols = [c["name"] for c in cfgs if c.get("name")]
            if REQUIRED_AUTO_MAP_FIELD not in new_cols:
                QMessageBox.warning(self, "设置无效", f"必须包含列：{REQUIRED_AUTO_MAP_FIELD}")
                return
            self.output_cols = new_cols
            self.column_cfgs = cfgs
            # 同步关键字设置（在“映射框设置”里维护）
            global KEYWORDS
            self.keywords = dlg.get_keywords()
            KEYWORDS = self.keywords
            save_keywords_to_settings(self.settings, self.keywords)
            self.manual_values = self._init_manual_values(self.column_cfgs)
            self._save_column_cfgs()
            self._refresh_mapping_table_headers()
            self._log("[INFO] 已保存映射框设置。")

    
    def _open_keyword_settings(self):
        global KEYWORDS
        dlg = KeywordSettingsDialog(self, self.keywords)
        if dlg.exec() == QDialog.Accepted:
            self.keywords = dlg.get_keywords()
            KEYWORDS = self.keywords  # 更新全局评分规则
            save_keywords_to_settings(self.settings, self.keywords)
            self._log("[INFO] 已保存关键字设置。")
            # 立即刷新当前结构组的候选推荐
            if getattr(self, "groups", None) and self.group_selector.currentData():
                self._load_group_mapping(self.group_selector.currentData())


    def _open_help(self):
        dlg = QDialog(self)
        dlg.setWindowTitle("使用说明")
        dlg.resize(900, 560)

        root = QVBoxLayout(dlg)
        tabs = QTabWidget()
        root.addWidget(tabs, 1)

        def make_tab(html: str) -> QWidget:
            w = QWidget()
            lay = QVBoxLayout(w)
            view = QTextBrowser()
            view.setOpenExternalLinks(True)
            view.setHtml(html)
            lay.addWidget(view, 1)
            return w

        tabs.addTab(make_tab("""
        <h3>快速开始</h3>
        <ol>
          <li>添加文件：拖拽到左侧区域，或点击“添加文件”。</li>
          <li>分析文件：点击“分析文件并生成映射建议”。程序会按“表结构”自动分组。</li>
          <li>确认映射：在“列映射”区域选择结构组，检查每个输出列的映射/默认值。</li>
          <li>保存方案：点击“保存方案/另存为”，给方案起个名字（以后可一键复用）。</li>
          <li>开始处理：选择输出模式后点击“开始处理”。</li>
        </ol>
        <p><b>小技巧：</b>下拉候选只展示“相关列”(score&gt;0)。若候选太少，请检查“关键字设置”。</p>
        """), "快速开始")

        tabs.addTab(make_tab("""
        <h3>常见规则</h3>
        <ul>
          <li><b>海关提单数</b>：若源表没有相关列（选择“无”），会按“同公司出现行数”自动求和。</li>
          <li><b>国家</b>：若源表存在列名精确等于“国家”，会优先默认映射；也可切换手动输入/留空。</li>
          <li><b>数据来源</b>：行级来源 =（表内“数据来源”列）+（文件名去后缀）；多文件合并时会自动去重并用“|”连接。</li>
        </ul>
        """), "规则说明")

        tabs.addTab(make_tab("""
        <h3>界面入口在哪里？</h3>
        <ul>
          <li><b>映射框设置</b>：在“列映射”区域顶部按钮。可改输出列顺序、宽度、模式、默认值，并进入关键字设置。</li>
          <li><b>关键字设置</b>：在“映射框设置”里打开。支持负权重（用于排除 address/phone/id 等无关列）。</li>
          <li><b>管理方案</b>：在“列映射”区域的“管理”按钮里重命名/删除/设为默认。</li>
        </ul>
        """), "常见问题")

        btns = QHBoxLayout()
        root.addLayout(btns)
        btns.addStretch(1)
        btn_close = QPushButton("关闭")
        btn_close.clicked.connect(dlg.accept)
        btns.addWidget(btn_close)

        dlg.exec()


    def _analyze_files(self):
        if self.file_list.count() == 0:
            return

        self.status_label.setText("状态：分析中…")
        self.progress.setValue(0)
        self._log("[STEP] 开始分析文件：识别表头、提取列名、分组结构。")

        analyses: List[FileAnalysis] = []
        errors = 0

        for i in range(self.file_list.count()):
            path = Path(self.file_list.item(i).data(Qt.UserRole))
            try:
                raw = read_raw_table(path)
                header_row = detect_header_row(raw)
                df, cols = build_table_from_raw(raw, header_row)
                fp = columns_fingerprint(cols)
                analyses.append(FileAnalysis(path=path, header_row=header_row, columns=cols, fingerprint=fp))
                self._log(f"[OK] {path.name}：header_row={header_row}, 列数={len(cols)}, 结构={fp[:8]}")
            except Exception as e:
                errors += 1
                self._log(f"[ERR] {path.name} 分析失败：{e}")

        self.file_analyses = analyses
        self.groups = self._group_by_fingerprint(analyses)

        self.group_selector.blockSignals(True)
        self.group_selector.clear()
        for g in self.groups:
            self.group_selector.addItem(f"{g.fingerprint[:8]}（{len(g.files)}个文件）", g.fingerprint)
        self.group_selector.blockSignals(False)

        if self.groups:
            self.group_selector.setCurrentIndex(0)
            self._load_group_mapping(self.groups[0].fingerprint)

        self.status_label.setText("状态：分析完成" + (f"（有 {errors} 个文件失败）" if errors else ""))
        self._update_buttons()

    def _group_by_fingerprint(self, analyses: List[FileAnalysis]) -> List[GroupAnalysis]:
        mp: Dict[str, List[FileAnalysis]] = {}
        cols_map: Dict[str, List[str]] = {}
        for a in analyses:
            mp.setdefault(a.fingerprint, []).append(a)
            cols_map[a.fingerprint] = a.columns
        return [GroupAnalysis(fingerprint=fp, columns=cols_map[fp], files=files) for fp, files in mp.items()]

    def _on_group_changed(self, idx: int):
        if idx < 0 or idx >= self.group_selector.count():
            return
        fp = self.group_selector.currentData()
        if fp:
            self._load_group_mapping(fp)

    def _load_group_mapping(self, fingerprint: str):
        group = next((g for g in self.groups if g.fingerprint == fingerprint), None)
        if not group:
            return

        self.current_group_fp = fingerprint


        # 先刷新当前结构组的“方案列表”
        self._refresh_profiles(fingerprint)

        mapping: Dict[str, str]
        if self.current_profile_name and self.current_profile_name != "__unsaved__":
            stored = self.mapping_store.get_profile(fingerprint, self.current_profile_name)
        else:
            stored = None

        if stored:
            mapping = stored
            self._log(f"[INFO] 已加载方案：{self.current_profile_name}（结构 {fingerprint[:8]}）")
        else:
            mapping = suggest_mapping(group.columns)
            self._log(f"[INFO] 已生成自动映射建议：结构 {fingerprint[:8]}")

        self.group_mappings.setdefault(fingerprint, {})
        self.group_mappings[fingerprint].update(mapping)

        file_names = ", ".join([f.path.name for f in group.files[:3]])
        more = "" if len(group.files) <= 3 else f" 等{len(group.files)}个"
        self.group_info.setText(f"列数={len(group.columns)} | 示例文件：{file_names}{more}")

        self._render_mapping_row_for_group(group)

    def _render_mapping_row_for_group(self, group: GroupAnalysis):
        cfg_by = self._cfg_by_name()
        enabled_cols = [c for c in self.output_cols if cfg_by.get(c, {}).get("enabled", True)]

        for ci, out_col in enumerate(enabled_cols):
            cfg = cfg_by.get(out_col, {"mode": MODE_EMPTY, "default": ""})
            mode = cfg.get("mode", MODE_EMPTY)

            # 自动映射列：下拉框（候选列按得分过滤 + 排序）
            if mode == MODE_AUTO and out_col in AUTO_MAP_FIELDS:
                chosen = self.group_mappings.get(group.fingerprint, {}).get(out_col, "") or ""

                cand = candidate_columns_for_field(out_col, group.columns, chosen=chosen)
                candidates = ["无"] + cand  # “无”表示源表没有该列

                cb = QComboBox()
                cb.addItems(candidates)

                # 国家：如果源列中存在严格“国家”，优先选中它
                if out_col == "国家":
                    if "国家" in [normalize_spaces(x) for x in group.columns]:
                        chosen = "国家"

                if chosen and chosen in group.columns:
                    cb.setCurrentText(chosen)
                else:
                    cb.setCurrentIndex(0)

                cb.currentIndexChanged.connect(lambda _, col=out_col, gfp=group.fingerprint: self._on_mapping_changed(gfp, col))
                self.map_table.setCellWidget(0, ci, cb)
            else:
                # 非自动映射列：保持 _refresh_mapping_table_headers 的渲染
                # 如果该列还没渲染（例如 MODE_AUTO 但不是可映射字段），用 "-" 占位
                if self.map_table.cellWidget(0, ci) is None and self.map_table.item(0, ci) is None:
                    it = QTableWidgetItem("-")
                    it.setFlags(it.flags() & ~Qt.ItemIsEditable)
                    self.map_table.setItem(0, ci, it)

            # 样例行：只对自动映射列给预览；否则 "-"
            if mode == MODE_AUTO and out_col in AUTO_MAP_FIELDS:
                cb = self.map_table.cellWidget(0, ci)
                chosen2 = cb.currentText() if isinstance(cb, QComboBox) else "无"
                if chosen2 == "无":
                    it = QTableWidgetItem("-")
                else:
                    it = QTableWidgetItem(self._preview_column(group, chosen2))
                it.setFlags(it.flags() & ~Qt.ItemIsEditable)
                self.map_table.setItem(1, ci, it)

        self._apply_column_widths()
        self.map_table.resizeRowsToContents()

    def _refresh_profiles(self, fingerprint: str):
        # 方案列表（按结构保存）
        names = self.mapping_store.list_profiles(fingerprint)
        self.profile_selector.blockSignals(True)
        self.profile_selector.clear()
        if names:
            for n in names:
                self.profile_selector.addItem(n, n)
            last = self.mapping_store.get_last_used(fingerprint)
            if last and last in names:
                self.profile_selector.setCurrentIndex(names.index(last))
            else:
                self.profile_selector.setCurrentIndex(0)
            self.current_profile_name = safe_str(self.profile_selector.currentData())
        else:
            self.profile_selector.addItem("（未保存）", "__unsaved__")
            self.profile_selector.setCurrentIndex(0)
            self.current_profile_name = "__unsaved__"
        self.profile_selector.blockSignals(False)

    def _on_profile_changed(self, idx: int = 0):
        fp = self.current_group_fp
        if not fp:
            return
        name = safe_str(self.profile_selector.currentData())
        self.current_profile_name = name
        if not name or name == "__unsaved__":
            # 未保存方案：保持当前映射（通常是自动建议/手动调整）
            self._log("[INFO] 当前为未保存方案。需要保存请点“保存方案/另存为”。")
            return
        stored = self.mapping_store.get_profile(fp, name)
        if not stored:
            return
        self.mapping_store.set_last_used(fp, name)
        self.group_mappings[fp] = stored
        self._refresh_mapping_table_for_current_group()
        self._log(f"[INFO] 已切换方案：{name}")

    def _save_profile(self):
        fp = self.current_group_fp
        if not fp:
            return
        mapping = self.group_mappings.get(fp, {})
        name = safe_str(self.profile_selector.currentData())
        if not name or name == "__unsaved__":
            # 需要命名
            name, ok = QInputDialog.getText(self, "保存方案", "方案名称：", text="方案1")
            if not ok:
                return
            name = safe_str(name)
            if not name:
                return
        self.mapping_store.save_profile(fp, name, mapping)
        self.mapping_store.set_last_used(fp, name)
        self._refresh_profiles(fp)
        # 选中刚保存的
        idx = self.profile_selector.findText(name)
        if idx >= 0:
            self.profile_selector.setCurrentIndex(idx)
        QMessageBox.information(self, "已保存", f"已保存方案：{name}")

    def _save_as_profile(self):
        fp = self.current_group_fp
        if not fp:
            return
        mapping = self.group_mappings.get(fp, {})
        name, ok = QInputDialog.getText(self, "另存为方案", "方案名称：", text="方案2")
        if not ok:
            return
        name = safe_str(name)
        if not name:
            return
        # 同名确认覆盖
        if name in self.mapping_store.list_profiles(fp):
            if QMessageBox.question(self, "覆盖确认", f"方案“{name}”已存在，是否覆盖？") != QMessageBox.Yes:
                return
        self.mapping_store.save_profile(fp, name, mapping)
        self.mapping_store.set_last_used(fp, name)
        self._refresh_profiles(fp)
        idx = self.profile_selector.findText(name)
        if idx >= 0:
            self.profile_selector.setCurrentIndex(idx)
        QMessageBox.information(self, "已保存", f"已另存为方案：{name}")

    def _manage_profiles(self):
        fp = self.current_group_fp
        if not fp:
            return
        current = safe_str(self.profile_selector.currentData())
        dlg = ProfileManagerDialog(self, self.mapping_store, fp, current if current and current != "__unsaved__" else "")
        dlg.exec()
        # 刷新并尽量保持选中
        keep = dlg.get_current()
        self._refresh_profiles(fp)
        if keep:
            idx = self.profile_selector.findText(keep)
            if idx >= 0:
                self.profile_selector.setCurrentIndex(idx)

    # 兼容旧按钮/旧调用
    def _save_current_mapping(self):
        self._save_profile()

    def _refresh_mapping_table_for_current_group(self):
        fp = self.current_group_fp
        if not fp:
            return
        group = next((g for g in self.groups if g.fingerprint == fp), None)
        if not group:
            return
        self._render_mapping_table(group)


    def _on_mapping_changed(self, fingerprint: str, out_col: str):
        group = next((g for g in self.groups if g.fingerprint == fingerprint), None)
        if not group:
            return
        cfg_by = self._cfg_by_name()
        enabled_cols = [c for c in self.output_cols if cfg_by.get(c, {}).get("enabled", True)]
        if out_col not in enabled_cols:
            return
        ci = enabled_cols.index(out_col)

        cb = self.map_table.cellWidget(0, ci)
        if not isinstance(cb, QComboBox):
            return
        chosen = cb.currentText()
        self.group_mappings.setdefault(fingerprint, {})
        self.group_mappings[fingerprint][out_col] = "" if chosen == "无" else chosen

        # 更新样例行（第1行）
        if chosen == "无":
            it = QTableWidgetItem("-")
        else:
            it = QTableWidgetItem(self._preview_column(group, chosen))
        it.setFlags(it.flags() & ~Qt.ItemIsEditable)
        self.map_table.setItem(1, ci, it)

    def _preview_column(self, group: GroupAnalysis, chosen: str) -> str:
        fa = group.files[0]
        try:
            raw = read_raw_table(fa.path)
            df, cols = build_table_from_raw(raw, fa.header_row)
            if chosen not in df.columns:
                return "（该列不存在）"
            vals = [safe_str(x) for x in df[chosen].head(SAMPLE_PREVIEW_N).tolist()]
            vals = [v if v else "（空）" for v in vals]
            if not vals:
                return "（空）"
            # 更清晰的分隔（并避免自动换行：已在表格禁用 wordwrap）
            sep = "\n────────\n"
            lines = [f"[{i+1}] {v}" for i, v in enumerate(vals)]
            return sep.join(lines)
        except Exception as e:
            return f"（预览失败：{e}）"

    def _save_current_mapping(self):
        fp = self.current_group_fp
        if not fp:
            return
        mapping = self.group_mappings.get(fp, {})
        self.mapping_store.set(fp, mapping)
        QMessageBox.information(self, "已保存", f"已保存该表结构的映射记忆（{fp[:8]}）。")

    def _start(self):
        if not self.groups:
            QMessageBox.warning(self, "提示", "请先点击“分析文件并生成映射建议”。")
            return

        out_dir = self._get_output_dir()
        if not out_dir:
            QMessageBox.warning(self, "提示", "请先选择输出目录。")
            return

        cfg_by = self._cfg_by_name()
        cfg = cfg_by.get(REQUIRED_AUTO_MAP_FIELD)
        if not cfg or not cfg.get("enabled", True) or cfg.get("mode") != MODE_AUTO:
            QMessageBox.warning(self, "设置不允许", "“客户名称”必须启用且模式为【自动映射】。")
            return

        for g in self.groups:
            mapping = self.group_mappings.get(g.fingerprint, {})
            if not mapping.get(REQUIRED_AUTO_MAP_FIELD):
                QMessageBox.warning(self, "映射缺失", f"结构组 {g.fingerprint[:8]} 缺少必需映射：客户名称")
                return

        merge_output = self.rb_merge.isChecked()

        self.btn_start.setEnabled(False)
        self.btn_analyze.setEnabled(False)
        self.btn_open_dir.setEnabled(False)
        self.status_label.setText("状态：处理中…")
        self.step_label.setText("步骤：-")
        self.progress.setValue(0)

        self.worker = Worker(
            groups=self.groups,
            group_mappings=self.group_mappings,
            output_columns=self.output_cols,
            column_cfgs=self.column_cfgs,
            manual_values=self.manual_values,
            output_dir=out_dir,
            merge_output=merge_output,
        )
        self.thread = QThread(self)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.progress.connect(self._on_progress)
        self.worker.step.connect(self._on_step)
        self.worker.log.connect(self._log)
        self.worker.output_ready.connect(self._on_output_ready)
        self.worker.output_file_ready.connect(self._on_output_file_ready)
        self.worker.finished.connect(self._on_finished)

        self.thread.start()

    def _on_progress(self, v: int):
        self.progress.setValue(v)

    def _on_step(self, s: str):
        self.step_label.setText(f"步骤：{s}")

    def _on_output_ready(self, out_dir: str):
        self._last_output_dir = out_dir
        self.btn_open_dir.setEnabled(True)

    def _on_output_file_ready(self, out_file: str):
        self._last_output_file = out_file

    def _on_finished(self, ok: bool, msg: str):
        try:
            self.thread.quit()
            self.thread.wait(3000)
        except Exception:
            pass

        self.btn_start.setEnabled(True)
        self.btn_analyze.setEnabled(True)
        self._update_buttons()

        self.progress.setValue(0)

        if ok:
            self.status_label.setText("状态：任务已完成")

            box = QMessageBox(self)
            box.setWindowTitle("完成")
            box.setIcon(QMessageBox.Information)
            box.setText(msg)

            btn_open_file = None
            out_file = safe_str(self._last_output_file)
            out_dir = safe_str(self._last_output_dir)

            if out_file and Path(out_file).exists():
                btn_open_file = box.addButton("打开文件", QMessageBox.AcceptRole)
                if not out_dir:
                    out_dir = str(Path(out_file).parent)

            btn_open_dir = None
            if out_dir and Path(out_dir).exists():
                btn_open_dir = box.addButton("打开文件夹", QMessageBox.AcceptRole)

            box.addButton("关闭", QMessageBox.RejectRole)
            box.exec()

            clicked = box.clickedButton()
            try:
                if btn_open_file is not None and clicked == btn_open_file:
                    QDesktopServices.openUrl(QUrl.fromLocalFile(out_file))
                elif btn_open_dir is not None and clicked == btn_open_dir:
                    QDesktopServices.openUrl(QUrl.fromLocalFile(out_dir))
            except Exception:
                pass
        else:
            self.status_label.setText("状态：失败")
            QMessageBox.critical(self, "失败", msg)


def main():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
