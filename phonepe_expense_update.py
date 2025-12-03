#!/usr/bin/env python3
"""
phonepe_expense_update_with_masks_v3.py

PhonePe importer that:
 - extracts masked mobile(s) from PDF header and attaches linked_mobile_number to records
 - looks up merchant/transfer target accounts by matching `accounts.locked_attributes->>'account_name'`
   against the parsed transaction `name` (case-insensitive, substring match)
 - looks up the SELF account per-record by matching `accounts.locked_attributes->>'mobile'`
   to the parsed `linked_mobile_number` (exact match, tries with/without leading +)
 - falls back to SURE_SELF_ACCOUNT_ID env or DEFAULT_SELF_ACCOUNT_ID if lookup fails

Place this file in the same environment as your original script and run:
  python phonepe_expense_update_with_masks_v3.py /path/to/statement.pdf [--min-date=YYYY-MM-DD] [--dry-run]

Based on your previous script. See original uploaded file for reference. fileciteturn3file0
"""
from __future__ import annotations

import csv
import getpass
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, getcontext
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Iterable

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from PyPDF2 import PdfReader, PdfWriter

# Decimal precision
getcontext().prec = 28

# Logging
LOG_FILE = os.path.abspath("phonepe_debug.log")
root_logger = logging.getLogger()
for h in list(root_logger.handlers):
    root_logger.removeHandler(h)
fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
fh.setLevel(logging.DEBUG)
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
fh.setFormatter(fmt)
sh.setFormatter(fmt)
root_logger.addHandler(fh)
root_logger.addHandler(sh)
root_logger.setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)
logger.info("Logging initialized. Writing to %s", LOG_FILE)

# Regexes
# Date-range header like: Oct 28, 2025 - Nov 27, 2025 (case-insensitive)
DATE_RANGE_RE = re.compile(
    r'\b[A-Za-z]{3,9}\s*\d{1,2},\s*\d{4}\b\s*[-–to]{1,4}\s*\b[A-Za-z]{3,9}\s*\d{1,2},\s*\d{4}\b',
    re.IGNORECASE,
)

DATE_FIND_RE = re.compile(
    r'([A-Za-z]{3,9}\s*\d{1,2},\s*\d{4}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}-\d{1,2}-\d{1,2})'
)
TIME_FIND_RE = re.compile(r'(\d{1,2}:\d{2}\s*(?:AM|PM))', re.IGNORECASE)
# INR_AMT_RE = re.compile(r'INR\s*([0-9,]+(?:\.[0-9]+)?)', re.IGNORECASE)
INR_AMT_RE = re.compile(
    r'(?:INR|₹|Rs\.?)\s*([0-9,]+(?:\.[0-9]+)?)',
    re.IGNORECASE,
)
AMOUNT_RE = re.compile(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)|\d+(?:\.[0-9]+)?)')

TXN_ID_RE = re.compile(r'Transaction\s*ID\s*[:\-\s]*([A-Za-z0-9]+)', re.IGNORECASE)
# UTR_RE = re.compile(r'UTR\s*No\s*[:\-\s]*([A-Za-z0-9]+)', re.IGNORECASE)
UTR_RE = re.compile(r'UTR\s*No\.?\s*[:\-\s]*([A-Za-z0-9]+)', re.IGNORECASE)
DEBIT_WORD_RE = re.compile(r'\bDebit\b', re.IGNORECASE)
CREDIT_WORD_RE = re.compile(r'\bCredit\b', re.IGNORECASE)
HEADER_KEYWORDS = ["date transaction details", "transaction details", "date transaction details type amount"]
# Header/header-context phrases to detect first-page header blocks
PAGE_HEADER_MARKERS = [
    r"transaction\s+statement\s+for",   # "Transaction Statement for +91..."
    r"transaction\s+details",          # the "Transaction Details" label
    r"date\s*$",                       # a lone "Date" line
]
PAGE_HEADER_MARKERS_RE = re.compile("|".join(PAGE_HEADER_MARKERS), re.IGNORECASE)


DEFAULT_SELF_ACCOUNT_ID = os.getenv("SURE_SELF_ACCOUNT_ID", "54f3d108-9ed2-446c-a489-ed1c2ffdf5b0")

# ----------------------
# Utilities & parsing
# ----------------------
def find_pdf2txt_cmd() -> Optional[str]:
    for name in ("pdf2txt.py", "pdf2txt"):
        p = shutil.which(name)
        if p:
            return p
    return None


def run_pdf2txt(pdf_path: Path, out_txt: Path) -> Path:
    cmd = find_pdf2txt_cmd()
    if not cmd:
        raise RuntimeError("pdf2txt utility not found in PATH. Install pdfminer.six.")
    try:
        subprocess.check_call([cmd, "-o", str(out_txt), str(pdf_path)])
    except Exception:
        with open(out_txt, "wb") as fh:
            subprocess.check_call([cmd, str(pdf_path)], stdout=fh)
    return out_txt


def decrypt_pdf_if_needed(pdf_path: Path) -> Path:
    reader = PdfReader(str(pdf_path))
    if not getattr(reader, "is_encrypted", False):
        return pdf_path
    for _ in range(3):
        pwd = getpass.getpass("PDF is encrypted. Enter password: ")
        try:
            if reader.decrypt(pwd):
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
                writer = PdfWriter()
                for p in reader.pages:
                    writer.add_page(p)
                with open(tmp.name, "wb") as fh:
                    writer.write(fh)
                return Path(tmp.name)
        except Exception:
            logger.exception("PDF decrypt attempt failed")
    raise RuntimeError("Failed to decrypt PDF after 3 attempts")


def normalize_date(dstr: str) -> str:
    if not dstr:
        return ""
    s = dstr.replace("\u00A0", " ").strip()
    s = re.sub(r",\s*(?=\d{4})", ", ", s)
    fmts = ["%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"]
    for f in fmts:
        try:
            return datetime.strptime(s, f).strftime("%Y-%m-%d")
        except Exception:
            pass
    nums = re.findall(r"\d+", s)
    if len(nums) >= 3:
        if len(nums[0]) == 4:
            y, m, d = nums[:3]
        else:
            d, m, y = nums[:3]
        try:
            return datetime(int(y), int(m), int(d)).strftime("%Y-%m-%d")
        except Exception:
            pass
    return s


def normalize_time(tstr: str) -> str:
    if not tstr:
        return ""
    t = tstr.strip().upper()
    for fmt in ("%I:%M %p", "%H:%M"):
        try:
            return datetime.strptime(t, fmt).strftime("%H:%M")
        except Exception:
            pass
    return t


def safe_decimal(s) -> Optional[Decimal]:
    if s is None or str(s).strip() == "":
        return None
    try:
        ss = str(s).replace(",", "").strip()
        d = Decimal(ss)
        return d.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    except InvalidOperation:
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)", str(s))
        if m:
            try:
                d = Decimal(m.group(1))
                return d.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            except InvalidOperation:
                return None
    return None


def parse_text_for_records(lines: List[str]) -> List[Dict]:
    pattern = re.compile(
        r'([A-Za-z]{3,9}\s*\d{1,2},\s*\d{4}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}-\d{1,2}-\d{1,2})\s+'
        r'(?:Paid|Add)\s*(?:to|money)\s*[:\-\s]*([A-Za-z0-9\s&]+)\s+'
        r'(DEBIT|CREDIT)\s+\s+'
        r'(?:INR|₹|Rs\.?)\s*([0-9,]+(?:\.[0-9]+)?)\s+'
        r'(\d{1,2}:\d{2}\s*(?:AM|PM))\s+'
        r'Transaction\s*ID\s*[:\-\s]*([A-Za-z0-9]+)\s+'
        r'UTR\s*No\.?\s*[:\-\s]*([A-Za-z0-9]+)\s+'
        r'(?:Paid|Debited|Credited)\s*(?:by|from|to)\s+(.+?)\s+'
        r'(?=(?:[A-Za-z]{3} \d{2}, \d{4}|Page|\Z))'
        , re.IGNORECASE | re.DOTALL
    )
    text = '\n'.join(lines)
    matches = pattern.findall(text)

    records = []
    for match in matches:
        # logger.info(match)

        # normalize date/time
        date_norm = normalize_date(match[0].strip())
        time_norm = normalize_time(match[4].strip())
        ts_time = time_norm or "00:00"
        try:
            created_dt = datetime.strptime(f"{date_norm} {ts_time}", "%Y-%m-%d %H:%M")
            created_at = created_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        except Exception:
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        record = {
            "date": date_norm,
            "time": time_norm,
            "created_at": created_at,
            "updated_at": updated_at,
            "name": match[1].strip() or "PhonePe",
            "transaction_id": match[5].strip() or None,
            "utr_no": match[6].strip() or None,
            "type": match[2].strip() or None,
            "amount": match[3].strip(),
            "paid_by": match[7].strip(),
        }
        records.append(record)

    return records


def parse_pdf2txt_lines(lines: List[str]) -> List[Dict]:
    """
    Robust parser that handles page breaks, tail-of-line details, and stray amount lines.
    Strategy:
      * Normalize page breaks and strip form-feed characters
      * Identify date line indices as anchors
      * Build blocks from date line tail + following lines up to next date index
      * Track which input line indices were consumed
      * After initial pass, find any leftover lines that look like transaction lines (contain amount/INR)
        and attach them to the nearest previous date anchor, then parse them into records.
      * Deduplicate records using (date, transaction_id, utr_no, amount) key.
    """
    records = []
    n = len(lines)

    # normalize lines: replace form-feed and CRs, keep mapping of normalized lines to original indices
    norm_lines = []
    orig_idx_map = []
    for idx, ln in enumerate(lines):
        if ln is None:
            ln = ""
        ln2 = ln.replace("\f", " ").replace("\r", " ").rstrip("\n")
        norm_lines.append(ln2)
        orig_idx_map.append(idx)

    # find indices with dates
    # date_indices = []
    # date_matches = {}
    # for idx, line in enumerate(norm_lines):
    #     ln = line.strip()
    #     m = DATE_FIND_RE.search(ln)
    #     if m:
    #         date_indices.append(idx)
    #         date_matches[idx] = m.group(1).strip()
    # find date anchors but skip date-range header lines and obvious page header contexts

    date_indices = []
    date_matches = {}
    for idx, line in enumerate(norm_lines):
        ln = line.strip()
        if not ln:
            continue

        # Skip lines that are explicit date-range headers like "Oct 28, 2025 - Nov 27, 2025"
        if 'DATE_RANGE_RE' in globals() and DATE_RANGE_RE.search(ln):
            continue

        # If the line itself is a lone header token like "Date" or "Transaction Details", skip it
        if ln.lower() in ("date", "transaction details", "date transaction details"):
            continue

        # If the nearby context indicates a page header, skip treating this as a date anchor.
        # Look up to 3 lines back and 2 lines ahead for header markers (masked mobile, "Transaction Statement for", etc.)
        context_window = " ".join(
            [norm_lines[i].strip() for i in range(max(0, idx-3), min(len(norm_lines), idx+3)) if norm_lines[i].strip()]
        )
        if 'PAGE_HEADER_MARKERS_RE' in globals() and PAGE_HEADER_MARKERS_RE.search(context_window):
            # This date is embedded in a page header block — ignore as anchor.
            continue

        # Normal date detection
        m = DATE_FIND_RE.search(ln)
        if m:
            date_indices.append(idx)
            date_matches[idx] = m.group(1).strip()


    if not date_indices:
        return records

    used_line_idxs = set()

    # helper to parse block_text (extract transaction fields)
    def parse_block_text(block_text, consumed_idxs) -> Optional[dict]:
        if not block_text or not block_text.strip():
            return None
        lowb = block_text.lower()
        if 'DATE_RANGE_RE' in globals() and DATE_RANGE_RE.search(block_text):
            return None

        # If the block appears to be part of a page header context, skip
        if 'PAGE_HEADER_MARKERS_RE' in globals() and PAGE_HEADER_MARKERS_RE.search(block_text):
            return None

        # If a known header/footer phrase exists in the block, truncate the block at its first occurrence
        first_pos = None
        for hk in HEADER_KEYWORDS:
            pos = lowb.find(hk)
            if pos != -1 and (first_pos is None or pos < first_pos):
                first_pos = pos
        if first_pos is not None:
            block_text = block_text[:first_pos].strip()
            lowb = block_text.lower()
        if not block_text:
            return None

        # extract payee
        paid_to = ""
        m_paid = re.search(
            r"(Paid to|Received from|Bill paid -|Bill paid)\s*(.+?)(?=(Transaction ID|UTR|INR|Debit|Credit|$))",
            block_text,
            re.IGNORECASE,
        )
        if m_paid:
            paid_to = m_paid.group(2).strip().rstrip(",")
        else:
            parts = re.split(r"Transaction\s*ID|UTR|INR|Debited\s*from|Credited\s*to|Debit|Credit", block_text, flags=re.IGNORECASE)
            paid_to = parts[0].strip().strip(" ,:-") if parts else ""

        # txn id and utr
        txn_id = ""
        mtx = TXN_ID_RE.search(block_text)
        if mtx:
            txn_id = mtx.group(1).strip()
        else:
            m_unl = re.search(r"\b([A-Z]{1,4}\d{5,}[A-Za-z0-9]*)\b", block_text)
            if m_unl:
                cand = m_unl.group(1)
                if len(cand) >= 6 and not re.match(r"^\d+$", cand):
                    txn_id = cand

        utr = ""
        mut = UTR_RE.search(block_text)
        if mut:
            utr = mut.group(1).strip()

        # amount and type
        txn_type = ""
        amount_txt = ""
        m_inr = INR_AMT_RE.search(block_text)
        if m_inr:
            amount_txt = m_inr.group(1).replace(",", "")
        else:
            ams = AMOUNT_RE.findall(block_text)
            if ams:
                amount_txt = ams[-1].replace(",", "")

        if DEBIT_WORD_RE.search(block_text):
            txn_type = "Debit"
        elif CREDIT_WORD_RE.search(block_text):
            txn_type = "Credit"

        amount_val = None
        if amount_txt:
            try:
                amount_val = f"{float(amount_txt):.2f}"
            except Exception:
                amount_val = amount_txt
        # sign convention
        if txn_type and txn_type.lower() == "debit" and amount_val:
            try:
                amount_val = f"-{abs(float(amount_val)):.2f}"
            except Exception:
                pass

        # try to find date token from nearby consumed indices by checking the original lines for date
        date_token = None
        for i in consumed_idxs:
            ln = norm_lines[i]
            m = DATE_FIND_RE.search(ln)
            if m:
                date_token = m.group(1).strip()
                break
        # fallback: try to find any date anywhere in block_text
        if not date_token:
            mdt = DATE_FIND_RE.search(block_text)
            if mdt:
                date_token = mdt.group(1).strip()

        if not date_token:
            # can't assign date reliably; skip
            return None

        # time extraction: look for time in consumed_idxs lines
        time_token = ""
        for i in consumed_idxs:
            ln = norm_lines[i]
            mt = TIME_FIND_RE.search(ln)
            if mt:
                time_token = mt.group(1).strip()
                break
        # fallback: from block_text
        if not time_token:
            mt = TIME_FIND_RE.search(block_text)
            if mt:
                time_token = mt.group(1).strip()

        # normalize date/time
        date_norm = normalize_date(date_token)
        time_norm = normalize_time(time_token)
        ts_time = time_norm or "00:00"
        try:
            created_dt = datetime.strptime(f"{date_norm} {ts_time}", "%Y-%m-%d %H:%M")
            created_at = created_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        except Exception:
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        rec = {
            "date": date_norm,
            "time": time_norm,
            "created_at": created_at,
            "updated_at": updated_at,
            "name": paid_to or "PhonePe",
            "transaction_id": txn_id or None,
            "utr_no": utr or None,
            "type": txn_type or None,
            "amount": amount_val,
            "_consumed_idxs": consumed_idxs.copy(),
        }
        return rec

    # initial pass: build blocks between date indices
    for pos_idx, start in enumerate(date_indices):
        end = date_indices[pos_idx + 1] if pos_idx + 1 < len(date_indices) else len(norm_lines)
        # collect indices from start to end-1
        idxs = list(range(start, end))
        # mark used lines
        for ii in idxs:
            used_line_idxs.add(ii)
        # build and parse block
        block_lines = [norm_lines[i] for i in idxs]
        block_text = " ".join([b.strip() for b in block_lines if b.strip()])
        rec = parse_block_text(block_text, idxs)
        if rec:
            records.append(rec)

    # secondary pass: find stray lines that contain amount or INR but were not used; attach to previous date anchor
    stray_amount_idxs = []
    for idx, ln in enumerate(norm_lines):
        if idx in used_line_idxs:
            continue
        if INR_AMT_RE.search(ln) or AMOUNT_RE.search(ln):
            if ln.strip():
                stray_amount_idxs.append(idx)

    # attach each stray line to nearest previous date index
    for sidx in stray_amount_idxs:
        # find previous date anchor index
        prev_dates = [d for d in date_indices if d < sidx]
        if not prev_dates:
            continue
        anchor = prev_dates[-1]
        # create consumed idx list: anchor..sidx
        idxs = list(range(anchor, sidx+1))
        # avoid reusing already used indices in this constructed block
        new_idxs = [i for i in idxs if i not in used_line_idxs]
        if not new_idxs:
            continue
        for ii in new_idxs:
            used_line_idxs.add(ii)
        block_lines = [norm_lines[i] for i in sorted(set([anchor] + new_idxs))]
        block_text = " ".join([b.strip() for b in block_lines if b.strip()])
        rec = parse_block_text(block_text, sorted(set([anchor] + new_idxs)))
        if rec:
            # avoid duplicates: check if same txn_id+date+amount exists
            dup = False
            for ex in records:
                if rec.get("transaction_id") and ex.get("transaction_id") and rec["transaction_id"] == ex["transaction_id"]:
                    dup = True; break
                if rec.get("amount") and ex.get("amount") and rec["amount"] == ex["amount"] and rec.get("date") == ex.get("date"):
                    dup = True; break
            if not dup:
                records.append(rec)

    # final: remove internal helper key before returning
    for r in records:
        if "_consumed_idxs" in r:
            del r["_consumed_idxs"]

    # sort records by date+time for predictability
    try:
        records.sort(key=lambda x: (x.get("date") or "", x.get("time") or ""))
    except Exception:
        pass


    # Final defensive filter: drop any parsed record whose name looks like a page header / date-range
    filtered = []
    for r in records:
        nm = (r.get('name') or '').strip()
        if not nm:
            continue
        if 'DATE_RANGE_RE' in globals() and DATE_RANGE_RE.search(nm):
            # skip header-like names
            continue
        if 'PAGE_HEADER_MARKERS_RE' in globals() and PAGE_HEADER_MARKERS_RE.search(nm):
            continue
        filtered.append(r)
    records = filtered

    return records

def parse_txt_file(path: Path) -> List[Dict]:
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        lines = [ln.rstrip("\n") for ln in fh]
    # return parse_pdf2txt_lines(lines)
    return parse_text_for_records(lines)


def extract_mobiles_from_pdf(pdf_path: Path) -> List[str]:
    try:
        reader = PdfReader(str(pdf_path))
        if not reader.pages:
            return []
        first_page = reader.pages[0]
        text = first_page.extract_text() or ""

        found = []
        pattern = re.compile(r"^Transaction Statement for\s+(\+?\d{10,15})")

        m = pattern.search(text)
        if m:
            mobile = m.group(1)
            print("Found:", mobile)
            found.append(mobile)
        return found
    except Exception:
        logger.exception("Failed extracting masked mobiles from PDF header")
        return []


def extract_masked_mobiles_from_pdf(pdf_path: Path) -> List[str]:
    try:
        reader = PdfReader(str(pdf_path))
        if not reader.pages:
            return []
        first_page = reader.pages[0]
        text = first_page.extract_text() or ""

        pat_full = re.compile(r"\+?\d{10,13}")
        pat_masked = re.compile(r"\+?\s*9?1?[0-9Xx\-\s]{6,}\d{2,4}", re.IGNORECASE)

        found = []
        for m in pat_full.finditer(text):
            s = m.group(0)
            s = re.sub(r"[^\d\+]", "", s)
            if s not in found:
                found.append(s)

        for m in pat_masked.finditer(text):
            s = m.group(0)
            s = re.sub(r"[ \-]", "", s)
            if s not in found:
                found.append(s)

        found = [re.sub(r"x", "X", f, flags=re.IGNORECASE) for f in found]
        return found
    except Exception:
        logger.exception("Failed extracting masked mobiles from PDF header")
        return []


def find_mask_in_text(block_text: str, masks: Iterable[str]) -> Optional[str]:
    if not block_text or not masks:
        return None
    low = block_text.lower()
    for m in masks:
        cmp_m = m.lower().replace("-", "").replace(" ", "")
        if cmp_m in re.sub(r"[ \-]", "", low):
            return m
    for m in masks:
        tail = re.search(r"(\d{3,4})$", m)
        if tail:
            t = tail.group(1)
            if re.search(r"\b" + re.escape(t) + r"\b", block_text):
                return m
    return None

# ----------------------
# DB helpers: lookup by mobile and by account_name in locked_attributes
# ----------------------
def connect_to_postgres():
    load_dotenv()
    host = os.getenv("SURE_DB_HOST") or os.getenv("DB_HOST") or "localhost"
    port = int(os.getenv("SURE_DB_PORT") or os.getenv("DB_PORT") or 5432)
    db = os.getenv("SURE_DB_NAME") or os.getenv("DB_NAME")
    user = os.getenv("SURE_DB_USER") or os.getenv("DB_USER")
    pw = os.getenv("SURE_DB_PASSWORD") or os.getenv("DB_PASSWORD")
    if not all([db, user, pw]):
        logger.error("Missing DB configuration in environment. Please set SURE_DB_NAME, SURE_DB_USER, SURE_DB_PASSWORD")
        return None
    try:
        conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)
        return conn
    except Exception:
        logger.exception("Database connection failed")
        return None


def lookup_self_account_from_paid_by(conn, paid_by: Optional[str]) ->Tuple[str, str]:
    if not paid_by:
        return None

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id,name FROM accounts WHERE locked_attributes->>'account_number' = %s LIMIT 1",
                (paid_by,),
            )
            row = cur.fetchone()
            if row:
                return (row[0],row[1])
    except Exception:
        logger.exception("lookup_self_account_from_paid_by failed")
    return None


def lookup_self_account_by_mobile(conn, mask: Optional[str]) ->Tuple[str, str]:
    """
    Find account.id where locked_attributes->>'mobile' = mask (tries with and without leading +).
    """
    if not mask:
        return None

    if len(mask) == 10:
        mask = '+91' + mask

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id,name FROM accounts WHERE locked_attributes->>'mobile' = %s LIMIT 1",
                (mask,),
            )
            row = cur.fetchone()
            if row:
                return (row[0],row[1])
            # try without '+'
            if mask.startswith("+"):
                cur.execute(
                    "SELECT id,name FROM accounts WHERE locked_attributes->>'mobile' = %s LIMIT 1",
                    (mask.lstrip("+"),),
                )
                row2 = cur.fetchone()
                if row2:
                    return (row2[0],row2[1])
    except Exception:
        logger.exception("lookup_self_account_by_mobile failed")
    return None


def lookup_account_by_name(conn, name: str) -> Optional[Dict]:
    """
    Lookup accounts table for a record whose locked_attributes->>'account_name' loosely matches `name`.
    Returns a dict with account id and account_name if found, otherwise None.

    Uses case-insensitive substring match via ILIKE.
    """
    if not name:
        return None
    try:
        with conn.cursor() as cur:
            # pattern = f"%{name.strip().lower()}%"
            pattern = f"{name.strip().lower()}"
            # Use lower(...) comparison for portability
            cur.execute(
                "SELECT id, locked_attributes->>'account_name' AS account_name FROM accounts WHERE lower(locked_attributes->>'account_name') LIKE %s LIMIT 1",
                (pattern,),
            )
            row = cur.fetchone()
            if row:
                return {"account_id": row[0], "account_name": row[1] or ""}
    except Exception:
        logger.exception("lookup_account_by_name failed")
    return None


def lookup_category_for_name_from_transactions(conn, name: str) -> Optional[str]:
    """
    Return the most common transactions.category_id for entries whose
    entryable is a transaction and whose entries.name matches `name`.

    Strategy:
      1) Exact case-insensitive match on entries.name
      2) Fallback: case-insensitive substring match on entries.name (LIKE '%name%')
      3) Return the category_id with the highest count, or None if none found.
    """
    if not name:
        return None

    try:
        with conn.cursor() as cur:
            # 1) exact (case-insensitive) match; restrict to likely transaction entryable_types
            cur.execute(
                """
                SELECT t.category_id, COUNT(*) AS cnt
                FROM entries e
                JOIN transactions t ON e.entryable_id = t.id
                WHERE lower(e.name) = lower(%s)
                  AND t.category_id IS NOT NULL
                  AND (e.entryable_type ILIKE 'transaction' OR e.entryable_type ILIKE 'transactions' OR e.entryable_type ILIKE 'Transaction' OR e.entryable_type ILIKE 'Transactions')
                GROUP BY t.category_id
                ORDER BY cnt DESC
                LIMIT 1
                """,
                (name,),
            )
            row = cur.fetchone()
            if row and row[0]:
                return row[0]

            # 2) fallback: substring match using lower(name) LIKE %pattern%
            pattern = f"%{name.strip().lower()}%"
            cur.execute(
                """
                SELECT t.category_id, COUNT(*) AS cnt
                FROM entries e
                JOIN transactions t ON e.entryable_id = t.id
                WHERE lower(e.name) LIKE %s
                  AND t.category_id IS NOT NULL
                  AND (e.entryable_type ILIKE 'transaction' OR e.entryable_type ILIKE 'transactions' OR e.entryable_type ILIKE 'Transaction' OR e.entryable_type ILIKE 'Transactions')
                GROUP BY t.category_id
                ORDER BY cnt DESC
                LIMIT 1
                """,
                (pattern,),
            )
            row2 = cur.fetchone()
            if row2 and row2[0]:
                return row2[0]
    except Exception:
        # keep behavior consistent with your other helpers: log, rollback attempt, then return None
        try:
            logger.exception("lookup_category_for_name_from_transactions failed")
        except Exception:
            pass
        try:
            conn.rollback()
        except Exception:
            pass

    return None

def lookup_category_for_name(conn, name: str) -> Optional[str]:
    """
    Find the most common non-null category_id among existing entries with the same (or similar) name.
    Strategy:
      1. Try exact case-insensitive match on entries.name.
      2. If nothing found, try case-insensitive substring match (LIKE '%name%').
      3. Return the category_id with highest count, or None if none found.
    """
    if not name:
        return None
    try:
        with conn.cursor() as cur:
            # 1) exact (case-insensitive) match
            cur.execute(
                """
                SELECT category_id, COUNT(*) AS cnt
                FROM entries
                WHERE lower(name) = lower(%s) AND category_id IS NOT NULL
                GROUP BY category_id
                ORDER BY cnt DESC
                LIMIT 1
                """,
                (name,),
            )
            row = cur.fetchone()
            if row and row[0]:
                return row[0]

            # 2) fallback: substring match
            pattern = f"%{name.strip().lower()}%"
            cur.execute(
                """
                SELECT category_id, COUNT(*) AS cnt
                FROM entries
                WHERE lower(name) LIKE %s AND category_id IS NOT NULL
                GROUP BY category_id
                ORDER BY cnt DESC
                LIMIT 1
                """,
                (pattern,),
            )
            row2 = cur.fetchone()
            if row2 and row2[0]:
                return row2[0]
    except Exception:
        logger.exception("lookup_category_for_name failed")
        try:
            conn.rollback()
        except Exception:
            pass
    return None


def txn_exists(conn, txn_id: Optional[str], utr_no: Optional[str], account_id: Optional[str] = None) -> bool:
    q_by_both = """
        SELECT EXISTS (
            SELECT 1 FROM entries WHERE account_id = %s AND source = %s AND external_id = %s
        );
    """
    q_by_any = "SELECT EXISTS(SELECT 1 FROM entries WHERE source = %s OR external_id = %s);"
    try:
        with conn.cursor() as cur:
            if account_id and txn_id and utr_no:
                cur.execute(q_by_both, (account_id, txn_id, utr_no))
                return cur.fetchone()[0]
            cur.execute(q_by_any, (txn_id, utr_no))
            return cur.fetchone()[0]
    except psycopg2.errors.InFailedSqlTransaction:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("Connection was in failed transaction state; rolled back. Treating txn as not existing.")
        return False
    except Exception:
        logger.exception("Error checking txn existence; treating as not exists")
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def perform_transfer(conn, txn: Dict, self_account: Tuple[str, str]) -> Tuple[str, Optional[int], Optional[int]]:
    """
    Attempt to treat the transaction as an internal transfer by matching the payee name to an
    account stored in accounts.locked_attributes->>'account_name'. Uses self_account_id as the 'from' account.
    Returns ("created"/"exists"/"skip"/"error", outflow_txn_id, inflow_txn_id)
    """
    name = (txn.get("name") or "").strip()
    type = (txn.get("type") or "Debit").strip().lower()
    matched = lookup_account_by_name(conn, name)
    if not matched:
        return ("skip", None, None)

    to_account = matched["account_id"]
    to_name = matched["account_name"]
    from_account = self_account[0] or os.getenv("SURE_SELF_ACCOUNT_ID") or DEFAULT_SELF_ACCOUNT_ID
    from_name = self_account[1]

    amt = safe_decimal(txn.get("amount"))
    if amt is None:
        return ("skip", None, None)

    # if int(amt) > 0:
    out_amount = amt
    in_amount = -amt
    if type == "credit":
        # incoming to self -> swap
        tmp_id, tmp_name = to_account, to_name
        to_account, to_name = from_account, from_name
        from_account, from_name = tmp_id, tmp_name
        out_amount = -amt
        in_amount = amt

    source_out = txn.get("transaction_id") or f"PHONEPE-{txn.get('created_at')}"
    source_in = f"{source_out}_IN"
    external_id = txn.get("utr_no")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT outflow_transaction_id, inflow_transaction_id
                FROM transfers
                WHERE outflow_transaction_id IN (
                    SELECT entryable_id FROM entries WHERE source = %s
                );
                """,
                (source_out,),
            )
            row = cur.fetchone()
            if row:
                conn.commit()
                return ("exists", row[0], row[1])

            cur.execute("BEGIN;")

            cur.execute(
                "INSERT INTO transactions (created_at, updated_at, kind, external_id) VALUES (%s, %s, 'funds_movement', %s) RETURNING id",
                (txn["created_at"], txn["updated_at"], source_out),
            )
            out_txn_id = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO entries (
                    account_id, entryable_type, entryable_id, amount, currency, date, name,
                    created_at, updated_at, notes, locked_attributes, external_id, source
                ) VALUES (%s, 'Transaction', %s, %s, 'INR', %s, %s, %s, %s, %s, '{}'::jsonb, %s, %s)
                RETURNING id
                """,
                (
                    from_account,
                    out_txn_id,
                    out_amount,
                    txn["date"],
                    f"Transfer to {to_name}",
                    txn["created_at"],
                    txn["updated_at"],
                    "Imported via PhonePe transfer automation",
                    external_id,
                    source_out,
                ),
            )

            cur.execute(
                "INSERT INTO transactions (created_at, updated_at, kind, external_id) VALUES (%s, %s, 'funds_movement', %s) RETURNING id",
                (txn["created_at"], txn["updated_at"], source_in),
            )
            in_txn_id = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO entries (
                    account_id, entryable_type, entryable_id, amount, currency, date, name,
                    created_at, updated_at, notes, locked_attributes, external_id, source
                ) VALUES (%s, 'Transaction', %s, %s, 'INR', %s, %s, %s, %s, %s, '{}'::jsonb, %s, %s)
                RETURNING id
                """,
                (
                    to_account,
                    in_txn_id,
                    in_amount,
                    txn["date"],
                    f"Transfer from {from_name}",
                    txn["created_at"],
                    txn["updated_at"],
                    "Imported via PhonePe transfer automation",
                    external_id,
                    source_in,
                ),
            )

            cur.execute(
                "INSERT INTO transfers (outflow_transaction_id, inflow_transaction_id, status, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
                (out_txn_id, in_txn_id, "confirmed", txn["created_at"], txn["updated_at"]),
            )

            cur.execute("COMMIT;")
            return ("created", out_txn_id, in_txn_id)
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception("perform_transfer failed: %s", e)
        return ("error", str(e), None)


def insert_transactions(conn, records: List[Dict], min_date=None, dry_run=False) -> int:
    inserted = 0
    dry_rows = []

    try:
        for r in records:
            # date validation
            try:
                if not r.get("date"):
                    logger.warning("Missing date, skipping record: %s", r)
                    continue
                txn_date = datetime.strptime(r["date"], "%Y-%m-%d").date()
            except Exception:
                logger.warning("Invalid date, skipping: %s", r)
                continue
            if min_date and txn_date < min_date:
                continue

            # Resolve self account by linked_mobile_number (primary) then fallback to env/default
            self_account_id = None
            if r.get("paid_by"):
                self_account_id, self_account_name = lookup_self_account_from_paid_by(conn, r.get("paid_by"))
            # if r.get("linked_mobile_number"):
            #     self_account_id, self_account_name = lookup_self_account_by_mobile(conn, r.get("linked_mobile_number"))
            if not self_account_id:
                self_account_id = os.getenv("SURE_SELF_ACCOUNT_ID") or DEFAULT_SELF_ACCOUNT_ID
                self_account_name = "SELF_ACCOUNT"

            exists = txn_exists(conn, r.get("transaction_id"), r.get("utr_no"), self_account_id)
            if exists:
                logger.info("Already exists, skipping: source=%s external_id=%s account=%s", r.get("transaction_id"), r.get("utr_no"), self_account_id)
                continue

            amt_dec = safe_decimal(r.get("amount"))
            if amt_dec is None:
                logger.warning("Invalid amount, skipping: %s", r)
                continue

            # Try internal transfer using DB-based account lookup
            transfer_result = perform_transfer(conn, r, (self_account_id,self_account_name))
            if transfer_result[0] == "created":
                logger.info("Inserted transfer for source=%s, amount=%s", r.get("transaction_id"), r.get("amount"))
                continue
            elif transfer_result[0] == "exists":
                logger.info("Transfer exists for source=%s, amount=%s", r.get("transaction_id"), r.get("amount"))
                continue
            elif transfer_result[0] == "error":
                logger.error("Transfer error, will try as expense: %s", transfer_result[1])

            # Normal expense insertion path
            locked_attrs = {}
            if r.get("linked_mobile_number"):
                locked_attrs["mobile"] = r.get("linked_mobile_number")
            locked_attrs["parser_version"] = "phonepe-v3"

            row = {
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
                "date": r["date"],
                "name": r.get("name") or "PhonePe",
                "amount": abs(amt_dec),
                "external_id": r.get("utr_no"),
                "source": r.get("transaction_id"),
                "linked_mobile_number": r.get("linked_mobile_number"),
                "self_account_id": self_account_id,
                "locked_attributes": locked_attrs,
            }

            if dry_run:
                dry_rows.append(row)
                continue

            try:
                # with conn.cursor() as cur:
                #     cur.execute(
                #         """
                #         INSERT INTO transactions (created_at, updated_at, category_id, merchant_id, locked_attributes, kind, external_id)
                #         VALUES (%s, %s, NULL, NULL, %s::jsonb, 'standard', %s)
                #         RETURNING id
                #         """,
                #         (row["created_at"], row["updated_at"], json.dumps(row["locked_attributes"]), row["source"]),
                #     )
                #     trans_id = cur.fetchone()[0]
                #

                # try to inherit category from previous entries with same name
                with conn.cursor() as cur:
                    category_id = lookup_category_for_name_from_transactions(conn, row["name"])
                    cur.execute(
                        """
                        INSERT INTO transactions (created_at, updated_at, category_id, merchant_id, locked_attributes, kind, external_id)
                        VALUES (%s, %s, %s, NULL, %s::jsonb, 'standard', %s)
                        RETURNING id
                        """,
                        (row["created_at"], row["updated_at"], category_id, json.dumps(row["locked_attributes"]), row["source"]),
                    )
                    trans_id = cur.fetchone()[0]

                with conn.cursor() as cur:

                    cur.execute(
                        """
                        INSERT INTO entries (
                            account_id, entryable_type, entryable_id, amount, currency, date, name,
                            created_at, updated_at, import_id, notes, excluded, plaid_id, locked_attributes, external_id, source
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, false, NULL, %s::jsonb, %s, %s)
                        RETURNING id
                        """,
                        (
                            row["self_account_id"],
                            "Transaction",
                            trans_id,
                            row["amount"],
                            "INR",
                            row["date"],
                            row["name"],
                            row["created_at"],
                            row["updated_at"],
                            "Added via automation-script",
                            json.dumps(row["locked_attributes"]),
                            row["external_id"],
                            row["source"],
                        ),
                    )
                    entry_id = cur.fetchone()[0]
                    conn.commit()
                    inserted += 1
                    logger.info("Inserted expense entry id=%s trans=%s amount=%s account=%s mask=%s", entry_id, trans_id, row["amount"], row["self_account_id"], row.get("linked_mobile_number"))
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logger.exception("Failed to insert expense row: %s", row)
                continue
    except Exception:
        logger.exception("Exception arise")
    finally:
        pass

    if dry_run and dry_rows:
        out_path = Path("phonepe_parsed_dryrun.csv")
        with out_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(dry_rows[0].keys()))
            writer.writeheader()
            writer.writerows(dry_rows)
        logger.info("Dry-run CSV written to %s (%d rows)", out_path, len(dry_rows))

    logger.info("Done. Inserted %d new expense rows", inserted)
    return inserted

def main():
    if len(sys.argv) < 2:
        print("Usage: python phonepe_expense_update_with_masks_v3.py input.pdf|input.txt [--min-date=YYYY-MM-DD] [--dry-run]")
        return

    inp = Path(sys.argv[1])
    min_date = None
    dry_run = False
    for a in sys.argv[2:]:
        if a.startswith("--min-date"):
            try:
                min_date = datetime.strptime(a.split("=", 1)[1], "%Y-%m-%d").date()
            except Exception:
                logger.error("Invalid --min-date value, expected YYYY-MM-DD")
                return
        elif a == "--dry-run":
            dry_run = True
        else:
            logger.warning("Unknown argument passed: %s", a)

    masks: List[str] = []
    parsed: List[Dict] = []

    if inp.suffix.lower() == ".pdf":
        pdf_to_parse = decrypt_pdf_if_needed(inp)
        # masks = extract_masked_mobiles_from_pdf(pdf_to_parse)
        masks = extract_mobiles_from_pdf(pdf_to_parse)
        logger.info("Found masked mobiles on page1: %s", masks)

        tmp = Path(tempfile.NamedTemporaryFile(delete=False, suffix=".txt").name)
        try:
            run_pdf2txt(pdf_to_parse, tmp)
            parsed = parse_txt_file(tmp)
        finally:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
    else:
        parsed = parse_txt_file(inp)
        try:
            with open(inp, "r", encoding="utf-8", errors="ignore") as fh:
                head_lines = [next(fh) for _ in range(200)]
            head = "".join(head_lines)
        except Exception:
            head = ""
        masks = re.findall(r"\+\s*(?:X|x|\d|[\s-]){6,}\d{2,4}", head)
        masks = [re.sub(r"[ \-]", "", m) for m in masks]
        masks = [re.sub(r"x", "X", f, flags=re.IGNORECASE) for f in masks]

    # attach linked_mobile_number metadata to records
    default_mask = masks[0] if len(masks) == 1 else None
    for rec in parsed:
        if default_mask:
            rec["linked_mobile_number"] = default_mask
        else:
            block_text = " ".join([str(rec.get(k) or "") for k in ("name", "transaction_id", "utr_no")])
            rec["linked_mobile_number"] = find_mask_in_text(block_text, masks)

    logger.info("Parsed %d records and attached linked_mobile_number metadata", len(parsed))

    conn = connect_to_postgres()
    if not conn:
        logger.error("DB connection failed; abort")
        if parsed:
            out_path = Path("phonepe_parsed_records.json")
            try:
                Path(out_path).write_text(json.dumps(parsed, indent=2, ensure_ascii=False), encoding="utf-8")
                logger.info("Wrote parsed records to %s for inspection", out_path)
            except Exception:
                pass
        return

    try:
        inserted = insert_transactions(conn, parsed, min_date=min_date, dry_run=dry_run)
        logger.info("Inserted %d rows", inserted)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
