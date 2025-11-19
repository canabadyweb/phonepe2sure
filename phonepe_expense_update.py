#!/usr/bin/env python3
"""
phonepe_expense_update.py

Usage:
    python phonepe_expense_update.py input.pdf
"""

import re
import sys
import os
import csv
import shutil
import subprocess
import tempfile
import getpass
from pathlib import Path
from datetime import datetime
import pandas as pd
from PyPDF2 import PdfReader, PdfWriter

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import ipdb

DATE_FIND_RE = re.compile(r'([A-Za-z]{3,9}\s*\d{1,2},\s*\d{4}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}-\d{1,2}-\d{1,2})')
TIME_FIND_RE = re.compile(r'(\d{1,2}:\d{2}\s*(?:AM|PM))', re.IGNORECASE)
AMOUNT_RE = re.compile(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)|\d+(?:\.[0-9]+)?)')
INR_AMT_RE = re.compile(r'INR\s*([0-9,]+(?:\.[0-9]+)?)', re.IGNORECASE)
TXN_ID_RE = re.compile(r'Transaction\s*ID\s*[:\-\s]*([A-Za-z0-9]+)', re.IGNORECASE)
UTR_RE = re.compile(r'UTR\s*No\s*[:\-\s]*([A-Za-z0-9]+)', re.IGNORECASE)
DEBIT_WORD_RE = re.compile(r'\bDebit\b', re.IGNORECASE)
CREDIT_WORD_RE = re.compile(r'\bCredit\b', re.IGNORECASE)


accounts = [
    {
        'account_name': 'PSG SBI AC',
        'account_id': '54f3d108-9ed2-446c-a489-ed1c2ffdf5b0',
        'transaction_name': 'SELF'
    },
    {
        'account_name': 'PSM AC',
        'account_id': '927a3ee1-3963-4ebf-97cd-137910e155c3',
        'transaction_name': 'P SELVAM'
    },
    {
        'account_name': 'PSK SBI AC',
        'account_id': '2afe1683-e05d-442f-ab1b-159d92d1b34e',
        'transaction_name': 'SELVAKUMARAN P'
    },
    {
        'account_name': 'PSS SBI AC',
        'account_id': 'f7b7839c-1fb4-429c-8088-441bf01a14a7',
        'transaction_name': 'SELVASANKAR P'
    },
    {
        'account_name': 'Dad SBI AC',
        'account_id': '7b58df97-381d-45a4-882e-d8364f793fbc',
        'transaction_name': 'PERUMAL R'
    },
    {
        'account_name': 'Cloudtree AC',
        'account_id': 'ca529321-4bf8-430c-aab5-90c333ca34a3',
        'transaction_name': 'CLOUDTREE'
    },
    {
        'account_name': 'PhonePe Wallet',
        'account_id': '340ca703-1057-4ce9-a038-730a26d20aea',
        'transaction_name': 'PhonePe Wallet'
    },
    {
        'account_name': 'PRIYA AGENCY (GAJENDIRAN) AC',
        'account_id': 'f2c6a294-db59-4336-88b4-c7f77298732b',
        'transaction_name': 'PRIYA AGENCY'
    },
]


def txn_exists(conn, txn):
    q = """
        SELECT EXISTS (
            SELECT 1
            FROM entries
            WHERE source = %s
               OR external_id = %s
        );
    """
    with conn.cursor() as cur:
        cur.execute(q, (txn['transaction_id'], txn['utr_no']))
        return cur.fetchone()[0]

def find_account_id(paid_to, accounts):
    if not paid_to:
        return None

    p = paid_to.lower().replace("  ", " ").strip()

    # 1. exact match
    for acc in accounts:
        if acc["transaction_name"].lower() == p:
            return acc["account_id"]

    # 2. substring match
    for acc in accounts:
        if acc["transaction_name"].lower() in p:
            return acc["account_id"]

    # 3. fuzzy word-level match
    pw = set(p.split())
    for acc in accounts:
        aw = set(acc["transaction_name"].lower().split())
        if aw.issubset(pw):
            return acc["account_id"]

    return None

def connect_to_postgres():
    """Establish and return a PostgreSQL connection."""

    # Load environment variables
    load_dotenv()

    try:
        conn = psycopg2.connect(
            host=os.getenv("SURE_DB_HOST"),
            port=os.getenv("SURE_DB_PORT"),
            database=os.getenv("SURE_DB_NAME"),
            user=os.getenv("SURE_DB_USER"),
            password=os.getenv("SURE_DB_PASSWORD"),
        )
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

def get_last_transaction_date(conn):
    """Fetch the last transaction date from the database to filter new tx."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(created_at) FROM entries;")
        last_date = cur.fetchone()[0]
        # print(last_date)
        # last_date = convert_to_date_format(
        #     last_date.strftime("%Y-%m-%d %H:%M:%S.%f"))
        # last_date = last_date.strftime("%Y-%m-%d %H:%M:%S.%f")
        # print(last_date.timestamp())
        # sys.exit(0)
        if last_date:
            return int(last_date.timestamp()+2000)
    return None

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
                "date": date_norm,
                "time": time_norm,
                "created_at": created_at,
                "updated_at": updated_at,
                "name": paid_to,
                "transaction_id": txn_id,
                "utr_no": utr,
                "type": dtype,
                "amount": amount
            })
    return records

def parse_txt_file(txt_path: Path):
    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.rstrip("\n") for ln in f]
    return parse_pdf2txt_lines(lines)

def insert_into_postgres(conn, transactions):
    """Insert transaction data into PostgreSQL."""

    with conn.cursor() as cur:
        for txn in transactions:

            txn_date = datetime.strptime(txn['date'], "%Y-%m-%d").date()
            given_date = datetime.strptime("2025-10-30", "%Y-%m-%d").date()
            if txn_date < given_date:
                continue

            # funds_movement

            self_account_id = '54f3d108-9ed2-446c-a489-ed1c2ffdf5b0'
            account_id = find_account_id(txn["name"], accounts)

            # ipdb.set_trace()

            # It is a transfer
            if account_id:
                # For transfer transactions, skip it as it doesn't work
                continue

                # # Insert into account_transactions
                # cur.execute("""
                #     INSERT INTO transactions ("created_at", "updated_at",
                #                               "category_id", "merchant_id",
                #                               "locked_attributes", "kind",
                #                               "external_id")
                #     VALUES (%s, %s, NULL, NULL, '{}', 'funds_movement', NULL)
                #     RETURNING id
                # """, (txn['created_at'], txn['updated_at']))
                # entryable_id = cur.fetchone()[0]
                #
                #
                # # Insert into account_entries
                #
                # if float(txn['amount']) > 0:
                #     # Inflow transfer
                #     cur.execute("""
                #         INSERT INTO entries (
                #             account_id, entryable_type, entryable_id, amount, currency, date, name,
                #             created_at, updated_at, import_id, notes, excluded, plaid_id, locked_attributes, external_id, source
                #         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, 'Added via automation-script', false, NULL, '{}', %s, %s)
                #     """, (account_id, 'Transaction', entryable_id, f"-{abs(float(txn['amount'])):.2f}", 'INR', txn['date'],
                #           f"Transfer from {txn['name']}", txn['created_at'], txn['updated_at'], txn['utr_no'], txn['transaction_id']))
                #
                #     cur.execute("""
                #         INSERT INTO entries (
                #             account_id, entryable_type, entryable_id, amount, currency, date, name,
                #             created_at, updated_at, import_id, notes, excluded, plaid_id, locked_attributes, external_id, source
                #         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, 'Added via automation-script', false, NULL, '{}', NULL, NULL)
                #     """, (self_account_id, 'Transaction', entryable_id, f"{abs(float(txn['amount'])):.2f}", 'INR', txn['date'],
                #           f"Transfer to PSG SBI AC", txn['created_at'], txn['updated_at']))
                # else:
                #     # Outflow transfer
                #     cur.execute("""
                #         INSERT INTO entries (
                #             account_id, entryable_type, entryable_id, amount, currency, date, name,
                #             created_at, updated_at, import_id, notes, excluded, plaid_id, locked_attributes, external_id, source
                #         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, 'Added via automation-script', false, NULL, '{}', NULL, NULL)
                #     """, (self_account_id, 'Transaction', entryable_id, f"{abs(float(txn['amount'])):.2f}", 'INR', txn['date'],
                #           f"Transfer from PSG SBI AC", txn['created_at'], txn['updated_at']))
                #
                #     cur.execute("""
                #         INSERT INTO entries (
                #             account_id, entryable_type, entryable_id, amount, currency, date, name,
                #             created_at, updated_at, import_id, notes, excluded, plaid_id, locked_attributes, external_id, source
                #         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, 'Added via automation-script', false, NULL, '{}', %s, %s)
                #     """, (account_id, 'Transaction', entryable_id, f"-{abs(float(txn['amount'])):.2f}", 'INR', txn['date'],
                #           f"Transfer to {txn['name']}", txn['created_at'], txn['updated_at'], txn['utr_no'], txn['transaction_id']))


            else:
                if txn_exists(conn, txn):
                    print(f"\nError: Transaction already exists: {txn}")
                else:
                    print(f"\nNew transaction: {txn}\n")

                    # Insert into account_transactions

                    # It is a standard expense
                    cur.execute("""
                        INSERT INTO transactions ("created_at", "updated_at",
                                                  "category_id", "merchant_id",
                                                  "locked_attributes", "kind",
                                                  "external_id")
                        VALUES (%s, %s, NULL, NULL, '{}', 'standard', NULL)
                        RETURNING id
                    """, (txn['created_at'], txn['updated_at']))
                    entryable_id = cur.fetchone()[0]

                    cur.execute("""
                        INSERT INTO entries (
                            account_id, entryable_type, entryable_id, amount, currency, date, name,
                            created_at, updated_at, import_id, notes, excluded, plaid_id, locked_attributes, external_id, source
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, 'Added via automation-script', false, NULL, '{}', %s, %s)
                    """, (self_account_id, 'Transaction', entryable_id, f"{abs(float(txn['amount'])):.2f}", 'INR', txn['date'],
                          txn['name'], txn['created_at'], txn['updated_at'], txn['utr_no'], txn['transaction_id']))

        conn.commit()

def main():
    if len(sys.argv) < 2:
        print("Usage: python phonepe_expense_update.py input.pdf")
        return

    inp = Path(sys.argv[1])
    # out = Path(sys.argv[2])
    out = "output.csv"

    if inp.suffix.lower() == ".pdf":
        pdf_to_parse = decrypt_pdf_if_needed(inp)
        tmp = Path(tempfile.NamedTemporaryFile(delete=False, suffix=".txt").name)
        run_pdf2txt(pdf_to_parse, tmp)
        records = parse_txt_file(tmp)
        tmp.unlink(missing_ok=True)
    else:
        records = parse_txt_file(inp)


    conn = connect_to_postgres()
    # ipdb.set_trace()
    if not conn:
        print("Postgres DB connection unsuccessful")
        return

    if records:

        # print(records)
        insert_into_postgres(conn, records)
        # print(f"Inserted {len(records)} transactions into PostgreSQL.")
    else:
        print("No new transactions found.")

    conn.close()

    # Save records to output.csv
    # df = pd.DataFrame(records, columns=[
    #     "Date","Time","created_at","updated_at",
    #     "Paid to","Transaction ID","UTR No","Debit/Credit","Amount"
    # ])
    # df.to_csv(out, index=False, quoting=csv.QUOTE_MINIMAL)
    # print(f"Saved {len(df)} rows -> {out}")

if __name__ == "__main__":
    main()
