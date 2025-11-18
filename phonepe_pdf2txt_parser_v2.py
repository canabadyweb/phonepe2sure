#!/usr/bin/env python3
"""
phonepe_pdf2txt_parser_v2.py

Same as previous version, but:
- created_at = parsed from Date + Time (YYYY-MM-DD HH:MM:SS.microseconds)
- updated_at = current timestamp only

Usage:
    python phonepe_pdf2txt_parser_v2.py input.pdf output.csv
    python phonepe_pdf2txt_parser_v2.py input.txt output.csv
"""

import re
import sys
import csv
import shutil
import subprocess
import tempfile
import getpass
from pathlib import Path
from datetime import datetime
import pandas as pd
from PyPDF2 import PdfReader, PdfWriter

DATE_FIND_RE = re.compile(r'([A-Za-z]{3,9}\s*\d{1,2},\s*\d{4}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}-\d{1,2}-\d{1,2})')
TIME_FIND_RE = re.compile(r'(\d{1,2}:\d{2}\s*(?:AM|PM))', re.IGNORECASE)
AMOUNT_RE = re.compile(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)|\d+(?:\.[0-9]+)?)')
INR_AMT_RE = re.compile(r'INR\s*([0-9,]+(?:\.[0-9]+)?)', re.IGNORECASE)
TXN_ID_RE = re.compile(r'Transaction\s*ID\s*[:\-\s]*([A-Za-z0-9]+)', re.IGNORECASE)
UTR_RE = re.compile(r'UTR\s*No\s*[:\-\s]*([A-Za-z0-9]+)', re.IGNORECASE)
DEBIT_WORD_RE = re.compile(r'\bDebit\b', re.IGNORECASE)
CREDIT_WORD_RE = re.compile(r'\bCredit\b', re.IGNORECASE)

def find_pdf2txt_cmd():
    for name in ("pdf2txt.py", "pdf2txt"):
        p = shutil.which(name)
        if p:
            return p
    return None

def run_pdf2txt(pdf_path: Path, out_txt: Path):
    cmd = find_pdf2txt_cmd()
    if not cmd:
        raise RuntimeError("pdf2txt not found in PATH")
    try:
        subprocess.check_call([cmd, "-o", str(out_txt), str(pdf_path)])
    except:
        with open(out_txt, "wb") as f:
            subprocess.check_call([cmd, str(pdf_path)], stdout=f)
    return out_txt

def decrypt_pdf_if_needed(pdf_path: Path):
    reader = PdfReader(str(pdf_path))
    if not getattr(reader, "is_encrypted", False):
        return pdf_path
    for _ in range(3):
        pwd = getpass.getpass("Enter PDF password: ")
        try:
            if reader.decrypt(pwd):
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
                writer = PdfWriter()
                for p in reader.pages:
                    writer.add_page(p)
                with open(tmp.name, "wb") as fh:
                    writer.write(fh)
                return Path(tmp.name)
        except:
            pass
    raise RuntimeError("Failed to decrypt PDF")

def normalize_date(d):
    if not d:
        return ""
    s = d.replace("\u00A0"," ").strip()
    s = re.sub(r',\s*(?=\d{4})', ', ', s)
    fmts = ["%b %d, %Y","%B %d, %Y","%Y-%m-%d","%d-%m-%Y","%d/%m/%Y"]
    for f in fmts:
        try:
            return datetime.strptime(s, f).strftime("%Y-%m-%d")
        except:
            pass
    nums = re.findall(r'\d+', s)
    if len(nums) >= 3:
        if len(nums[0]) == 4:
            y,m,d = nums[:3]
        else:
            d,m,y = nums[:3]
        try:
            return datetime(int(y), int(m), int(d)).strftime("%Y-%m-%d")
        except:
            pass
    return s

def normalize_time(t):
    if not t:
        return ""
    t = t.strip().upper()
    try:
        return datetime.strptime(t, "%I:%M %p").strftime("%H:%M")
    except:
        try:
            return datetime.strptime(t, "%H:%M").strftime("%H:%M")
        except:
            return t

def parse_pdf2txt_lines(lines):
    records = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i].strip()
        md = DATE_FIND_RE.search(line)
        if not md:
            i += 1
            continue
        date_token = md.group(1).strip()

        time_token = ""
        j = i + 1
        while j < n and not lines[j].strip():
            j += 1
        if j < n:
            mt = TIME_FIND_RE.search(lines[j].strip())
            if mt:
                time_token = mt.group(1).strip()
                j += 1

        block = []
        while j < n and not DATE_FIND_RE.search(lines[j]):
            block.append(lines[j])
            j += 1

        i = j

        block_text = " ".join([b.strip() for b in block if b.strip()])

        paid_to = ""
        m_paid = re.search(
            r'(Paid to|Received from|Bill paid -|Bill paid)\s*(.+?)(?=(Transaction ID|UTR|INR|Debit|Credit|$))',
            block_text, re.IGNORECASE
        )
        if m_paid:
            paid_to = m_paid.group(2).strip().rstrip(',')
        else:
            parts = re.split(r'Transaction\s*ID|UTR|INR|Debit|Credit', block_text, flags=re.IGNORECASE)
            paid_to = parts[0].strip().strip(' ,:-') if parts else ""

        txn_id = ""
        mtx = TXN_ID_RE.search(block_text)
        if mtx:
            txn_id = mtx.group(1).strip()

        utr = ""
        mut = UTR_RE.search(block_text)
        if mut:
            utr = mut.group(1).strip()

        dtype = ""
        amount = ""

        m_inr = INR_AMT_RE.search(block_text)
        if m_inr:
            amount = m_inr.group(1).replace(',', '')
            dtype = "Debit" if DEBIT_WORD_RE.search(block_text) else "Credit" if CREDIT_WORD_RE.search(block_text) else ""
        else:
            ams = AMOUNT_RE.findall(block_text)
            if ams:
                amount = ams[-1].replace(',', '')
            if DEBIT_WORD_RE.search(block_text):
                dtype = "Debit"
            elif CREDIT_WORD_RE.search(block_text):
                dtype = "Credit"

        if amount:
            try:
                amount = f"{float(amount):.2f}"
            except:
                pass

        if dtype.lower() == "debit" and amount:
            try:
                amount = f"-{abs(float(amount)):.2f}"
            except:
                pass

        date_norm = normalize_date(date_token)
        time_norm = normalize_time(time_token)

        # created_at from date+time
        ts_time = time_norm if time_norm else "00:00"
        try:
            created_dt = datetime.strptime(f"{date_norm} {ts_time}", "%Y-%m-%d %H:%M")
            created_at = created_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        except:
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        # To skip the first heading which is not a transaction record
        if "Date Transaction Details" not in paid_to:
            records.append({
                "Date": date_norm,
                "Time": time_norm,
                "created_at": created_at,
                "updated_at": updated_at,
                "Paid to": paid_to,
                "Transaction ID": txn_id,
                "UTR No": utr,
                "Debit/Credit": dtype,
                "Amount": amount
            })
    return records

def parse_txt_file(txt_path: Path):
    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.rstrip("\n") for ln in f]
    return parse_pdf2txt_lines(lines)

def main():
    if len(sys.argv) < 3:
        print("Usage: python phonepe_pdf2txt_parser_v2.py input.pdf|input.txt output.csv")
        return

    inp = Path(sys.argv[1])
    out = Path(sys.argv[2])

    if inp.suffix.lower() == ".pdf":
        pdf_to_parse = decrypt_pdf_if_needed(inp)
        tmp = Path(tempfile.NamedTemporaryFile(delete=False, suffix=".txt").name)
        run_pdf2txt(pdf_to_parse, tmp)
        records = parse_txt_file(tmp)
        tmp.unlink(missing_ok=True)
    else:
        records = parse_txt_file(inp)

    df = pd.DataFrame(records, columns=[
        "Date","Time","created_at","updated_at",
        "Paid to","Transaction ID","UTR No","Debit/Credit","Amount"
    ])
    df.to_csv(out, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Saved {len(df)} rows -> {out}")

if __name__ == "__main__":
    main()
