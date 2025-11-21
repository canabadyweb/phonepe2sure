
# --- Logging setup: file + console, non-destructive ---
import logging, os, json
LOG_FILE = os.path.abspath("phonepe_debug.log")
root_logger = logging.getLogger()
# remove existing handlers to avoid duplicates
for h in list(root_logger.handlers):
    root_logger.removeHandler(h)
# file handler (DEBUG)
fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
fh.setLevel(logging.DEBUG)
# console handler (INFO+)
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

"""
Fine-tuned PhonePe parser + Postgres inserter adapted to your DB schema.

Key changes from previous version:
- Matches your `transactions` and `entries` table schema:
  - transactions.external_id is set (we use transaction_id here)
  - entries.external_id is populated with UTR No; entries.source with Transaction ID
  - entries.account_id is required: use SURE_SELF_ACCOUNT_ID env or default
  - entries.amount passed as Decimal (psycopg2 will handle numeric(19,4))
  - entries.name is never null (fallback to 'PhonePe')
- Uses Decimal for safe money handling and keeps 4 decimal precision for DB (numeric(19,4))
- Avoids inserting duplicate (relies on txn_exists check and DB unique index)
- Dry-run option available to produce CSV of rows that would be inserted
- Better logging and error handling

Usage:
  pip install python-dotenv psycopg2-binary pandas PyPDF2 pdfminer.six
  python phonepe_expense_update_tuned_v2.py input.txt --min-date=2025-10-16 --dry-run
"""

import re
import sys
import os
import csv
import shutil
import subprocess
import tempfile
import getpass
import logging
from pathlib import Path
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, getcontext

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from PyPDF2 import PdfReader, PdfWriter

# from transfer_helper import perform_transfer_advanced

# set Decimal precision sufficiently high
getcontext().prec = 28



# regexes
DATE_FIND_RE = re.compile(r'([A-Za-z]{3,9}\s*\d{1,2},\s*\d{4}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}-\d{1,2}-\d{1,2})')
TIME_FIND_RE = re.compile(r'(\d{1,2}:\d{2}\s*(?:AM|PM))', re.IGNORECASE)
INR_AMT_RE = re.compile(r'INR\s*([0-9,]+(?:\.[0-9]+)?)', re.IGNORECASE)
AMOUNT_RE = re.compile(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)|\d+(?:\.[0-9]+)?)')
TXN_ID_RE = re.compile(r'Transaction\s*ID\s*[:\-\s]*([A-Za-z0-9]+)', re.IGNORECASE)
UTR_RE = re.compile(r'UTR\s*No\s*[:\-\s]*([A-Za-z0-9]+)', re.IGNORECASE)
DEBIT_WORD_RE = re.compile(r'\bDebit\b', re.IGNORECASE)
CREDIT_WORD_RE = re.compile(r'\bCredit\b', re.IGNORECASE)

HEADER_KEYWORDS = ["date transaction details", "transaction details", "date transaction details type amount"]

DEFAULT_SELF_ACCOUNT_ID = os.getenv("SURE_SELF_ACCOUNT_ID", "54f3d108-9ed2-446c-a489-ed1c2ffdf5b0")

accounts = [
    { 'account_name': 'PSG SBI AC', 'account_id': '54f3d108-9ed2-446c-a489-ed1c2ffdf5b0', 'transaction_name': 'SELF' },
    { 'account_name': 'PSM AC', 'account_id': '927a3ee1-3963-4ebf-97cd-137910e155c3', 'transaction_name': 'P SELVAM' },
    { 'account_name': 'PSK SBI AC', 'account_id': '2afe1683-e05d-442f-ab1b-159d92d1b34e', 'transaction_name': 'SELVAKUMARAN P' },
    { 'account_name': 'PSS SBI AC', 'account_id': 'f7b7839c-1fb4-429c-8088-441bf01a14a7', 'transaction_name': 'SELVASANKAR P' },
    { 'account_name': 'Dad SBI AC', 'account_id': '7b58df97-381d-45a4-882e-d8364f793fbc', 'transaction_name': 'PERUMAL R' },
    { 'account_name': 'Cloudtree AC', 'account_id': 'ca529321-4bf8-430c-aab5-90c333ca34a3', 'transaction_name': 'CLOUDTREE' },
    { 'account_name': 'PhonePe Wallet', 'account_id': '340ca703-1057-4ce9-a038-730a26d20aea', 'transaction_name': 'PhonePe Wallet' }
]

# helper funcs
def find_pdf2txt_cmd():
    for name in ("pdf2txt.py", "pdf2txt"):
        p = shutil.which(name)
        if p:
            return p
    return None

def run_pdf2txt(pdf_path: Path, out_txt: Path):
    cmd = find_pdf2txt_cmd()
    if not cmd:
        raise RuntimeError("pdf2txt utility not found in PATH. Install pdfminer.six.")
    try:
        subprocess.check_call([cmd, "-o", str(out_txt), str(pdf_path)])
    except Exception:
        with open(out_txt, "wb") as fh:
            subprocess.check_call([cmd, str(pdf_path)], stdout=fh)
    return out_txt

def decrypt_pdf_if_needed(pdf_path: Path):
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
            logging.exception("PDF decrypt attempt failed")
    raise RuntimeError("Failed to decrypt PDF after 3 attempts")

def normalize_date(dstr):
    if not dstr:
        return ""
    s = dstr.replace("\u00A0", " ").strip()
    s = re.sub(r',\s*(?=\d{4})', ', ', s)
    fmts = ["%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"]
    for f in fmts:
        try:
            return datetime.strptime(s, f).strftime("%Y-%m-%d")
        except Exception:
            pass
    nums = re.findall(r'\d+', s)
    if len(nums) >= 3:
        if len(nums[0]) == 4:
            y,m,d = nums[:3]
        else:
            d,m,y = nums[:3]
        try:
            return datetime(int(y), int(m), int(d)).strftime("%Y-%m-%d")
        except Exception:
            pass
    return s

def normalize_time(tstr):
    if not tstr:
        return ""
    t = tstr.strip().upper()
    try:
        return datetime.strptime(t, "%I:%M %p").strftime("%H:%M")
    except Exception:
        try:
            return datetime.strptime(t, "%H:%M").strftime("%H:%M")
        except Exception:
            return t

def safe_decimal(s):
    if s is None or str(s).strip() == "":
        return None
    try:
        # remove commas and whitespace
        ss = str(s).replace(",", "").strip()
        d = Decimal(ss)
        # quantize to 4 decimals for DB numeric(19,4)
        return d.quantize(Decimal('.0001'), rounding=ROUND_HALF_UP)
    except InvalidOperation:
        m = re.search(r'([0-9]+(?:\.[0-9]+)?)', str(s))
        if m:
            try:
                d = Decimal(m.group(1))
                return d.quantize(Decimal('.0001'), rounding=ROUND_HALF_UP)
            except InvalidOperation:
                return None
    return None

def parse_pdf2txt_lines(lines):
    records = []
    i = 0
    n = len(lines)
    while i < n:
        ln = lines[i].strip()
        m = DATE_FIND_RE.search(ln)
        if not m:
            i += 1; continue
        date_token = m.group(1).strip()
        # time may be next non-empty line
        time_token = ""
        j = i+1
        while j < n and not lines[j].strip():
            j += 1
        if j < n:
            mt = TIME_FIND_RE.search(lines[j].strip())
            if mt:
                time_token = mt.group(1).strip()
                j += 1
        # collect block until next date
        block = []
        while j < n and not DATE_FIND_RE.search(lines[j]):
            block.append(lines[j])
            j += 1
        i = j
        block_text = " ".join([b.strip() for b in block if b.strip()])
        if not block_text:
            continue
        lowb = block_text.lower()
        if any(hk in lowb for hk in HEADER_KEYWORDS):
            continue
        # Paid to
        paid_to = ""
        m_paid = re.search(r'(Paid to|Received from|Bill paid -|Bill paid)\s*(.+?)(?=(Transaction ID|UTR|INR|Debit|Credit|$))', block_text, re.IGNORECASE)
        if m_paid:
            paid_to = m_paid.group(2).strip().rstrip(',')
        else:
            parts = re.split(r'Transaction\s*ID|UTR|INR|Debited\s*from|Credited\s*to|Debit|Credit', block_text, flags=re.IGNORECASE)
            paid_to = parts[0].strip().strip(' ,:-') if parts else ""
        # txn id and utr
        txn_id = ""
        mtx = TXN_ID_RE.search(block_text)
        if mtx:
            txn_id = mtx.group(1).strip()
        else:
            m_unl = re.search(r'\b([A-Z]{1,4}\d{5,}[A-Za-z0-9]*)\b', block_text)
            if m_unl:
                cand = m_unl.group(1)
                if len(cand) >= 6 and not re.match(r'^\d+$', cand):
                    txn_id = cand
        utr = ""
        mut = UTR_RE.search(block_text)
        if mut:
            utr = mut.group(1).strip()
        # amount and type
        txn_type = ""
        amount = ""
        m_inr = INR_AMT_RE.search(block_text)
        if m_inr:
            amount = m_inr.group(1).replace(',', '')
            txn_type = "Debit" if DEBIT_WORD_RE.search(block_text) else "Credit" if CREDIT_WORD_RE.search(block_text) else ""
        else:
            ams = AMOUNT_RE.findall(block_text)
            if ams:
                amount = ams[-1].replace(',', '')
            if DEBIT_WORD_RE.search(block_text):
                txn_type = "Debit"
            elif CREDIT_WORD_RE.search(block_text):
                txn_type = "Credit"
        if amount:
            try:
                amount = f"{float(amount):.2f}"
            except Exception:
                pass
        if txn_type and txn_type.lower() == "debit" and amount:
            try:
                amount = f"-{abs(float(amount)):.2f}"
            except Exception:
                pass
        date_norm = normalize_date(date_token)
        time_norm = normalize_time(time_token)
        ts_time = time_norm if time_norm else "00:00"
        try:
            created_dt = datetime.strptime(f"{date_norm} {ts_time}", "%Y-%m-%d %H:%M")
            created_at = created_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        except Exception:
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        # skip header-like rows
        if paid_to and paid_to.lower().startswith("date transaction details"):
            continue
        records.append({
            "date": date_norm,
            "time": time_norm,
            "created_at": created_at,
            "updated_at": updated_at,
            "name": paid_to or "PhonePe",
            "transaction_id": txn_id,
            "utr_no": utr,
            "type": txn_type,
            "amount": amount
        })
    return records

def parse_txt_file(path: Path):
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        lines = [ln.rstrip("\n") for ln in fh]
    return parse_pdf2txt_lines(lines)

# DB helpers
def connect_to_postgres():
    load_dotenv()
    host = os.getenv("SURE_DB_HOST") or os.getenv("DB_HOST") or "localhost"
    port = int(os.getenv("SURE_DB_PORT") or os.getenv("DB_PORT") or 5432)
    db = os.getenv("SURE_DB_NAME") or os.getenv("DB_NAME")
    user = os.getenv("SURE_DB_USER") or os.getenv("DB_USER")
    pw = os.getenv("SURE_DB_PASSWORD") or os.getenv("DB_PASSWORD")
    if not all([db, user, pw]):
        logging.error("Missing DB configuration in environment. Please set SURE_DB_NAME, SURE_DB_USER, SURE_DB_PASSWORD")
        return None
    try:
        conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)
        return conn
    except Exception:
        logging.exception("Database connection failed")
        return None

def txn_exists(conn, txn_id, utr_no, account_id=None):
    """
    Safer existence check. Returns True if an entry already exists.
    If the connection is in a failed transaction state, it will rollback and return False.
    """
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
            cur.execute(q_by_any, (txn_id or None, utr_no or None))
            return cur.fetchone()[0]
    except psycopg2.errors.InFailedSqlTransaction:
        # connection is aborted due previous error; clear it and continue
        try:
            conn.rollback()
        except Exception:
            pass
        logging.warning("Connection was in failed transaction state; rolled back. Treating txn as not existing.")
        return False
    except Exception as e:
        logging.exception("Error checking txn existence; rolling back and treating as not exists: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return False

# Example wrapper that shows how to call perform_transfer_advanced safely
def handle_transfer_call(conn, r, accounts, perform_transfer_fn, self_account_id):
    try:
        res = perform_transfer_fn(conn, r, accounts, self_account_id=self_account_id)
    except Exception as ex:
        try:
            conn.rollback()
        except Exception:
            pass
        logging.exception("transfer helper raised unexpected exception; skipping row: %s", ex)
        return False

    if res.get("status") == "created":
        logging.info("Transfer created: %s", res)
        return True  # indicates row handled as transfer
    elif res.get("status") == "exists":
        logging.info("Transfer already exists: %s", res)
        return True
    elif res.get("status") == "skip":
        return False
    else:
        logging.error("Transfer failed or errored for row; skipping: %s", res)
        try:
            conn.rollback()
        except Exception:
            pass
        return False

# def txn_exists(conn, txn_id, utr_no, account_id):
#     # check existence by (account_id, source, external_id) if possible, otherwise by source or external_id
#     q_by_both = """
#         SELECT EXISTS (
#             SELECT 1 FROM entries WHERE account_id = %s AND source = %s AND external_id = %s
#         )
#     """
#     q_by_any = "SELECT EXISTS(SELECT 1 FROM entries WHERE source = %s OR external_id = %s);"
#     with conn.cursor() as cur:
#         if account_id and txn_id and utr_no:
#             cur.execute(q_by_both, (account_id, txn_id, utr_no))
#             res = cur.fetchone()[0]
#             if res:
#                 return True
#         cur.execute(q_by_any, (txn_id or None, utr_no or None))
#         return cur.fetchone()[0]

def perform_transfer(conn, txn, accounts):
    """
    Executes a fund transfer between two accounts inside the Sure DB.

    txn: parsed record from your PhonePe importer
         {
           "date": "2025-10-22",
           "time": "18:39",
           "transaction_id": "T2510221839280379821844",
           "utr_no": "638914984362",
           "amount": "-120.00",
           "name": "Paid to KBSKOFI BARR"
         }

    accounts: list of dicts
        [
          { 'account_name': 'PSM AC', 'account_id': 'uuid', 'transaction_name': 'P SELVAM' },
          ...
        ]

    Returns:
        ("created", outflow_txn_id, inflow_txn_id)
        ("exists", existing_outflow_id, existing_inflow_id)
        ("skip", reason)
    """

    name = (txn.get("name") or "").strip()

    # 1. Does txn match a known account? (Paying SELF or internal account)
    matched_account = None
    for acc in accounts:
        if acc["transaction_name"].lower() in name.lower():
            matched_account = acc
            break

    if not matched_account:
        return ("skip", "no_account_match")

    # The account we sent TO (destination)
    to_account = matched_account["account_id"]
    to_account_name = matched_account["account_name"]

    # The account FROM which money is debited (self account)
    from_account = DEFAULT_SELF_ACCOUNT_ID
    from_account_name = "PSG SBI AC"
    if not from_account:
        return ("skip", "self_account_missing")

    # Prepare schema-friendly values
    date = txn["date"]
    created_at = txn["created_at"]
    updated_at = txn["updated_at"]
    source_out = txn["transaction_id"]
    source_in = txn["transaction_id"] + "_IN"
    external_id = txn["utr_no"]
    notes = "Imported via PhonePe transfer automation"

    # Convert amount to Decimal
    from decimal import Decimal
    amount = Decimal(str(txn["amount"]).replace(",", "").strip())
    if amount > 0.0:
        # amount = -amount   # ensure outflow is always negative
        to_account_name = from_account_name
        from_account = matched_account["account_id"]
        from_account_name = matched_account["account_name"]
        to_account = DEFAULT_SELF_ACCOUNT_ID

    cursor = conn.cursor()

    # 2. Idempotency check — does transfer already exist?
    # query = f"""
    #     SELECT outflow_transaction_id, inflow_transaction_id
    #     FROM transfers
    #     WHERE
    #       outflow_transaction_id IN (
    #             SELECT entryable_id FROM entries WHERE source LIKE "%s")
    # """ % (source_out)
    query = """
           SELECT outflow_transaction_id, inflow_transaction_id
           FROM transfers
           WHERE outflow_transaction_id IN (
               SELECT entryable_id
               FROM entries
               WHERE source LIKE %s
           );
       """

    cursor.execute(query, (f"%{source_out}%",))
    row = cursor.fetchone()
    if row:
        conn.commit()
        return ("exists", row[0], row[1])

    # 3. Execute Atomic Transfer
    try:
        cursor.execute("BEGIN;")

        # --- Outflow Transaction (debit) ---
        cursor.execute("""
            INSERT INTO transactions (created_at, updated_at, kind, external_id)
            VALUES (%s, %s, 'funds_movement', %s)
            RETURNING id
        """, (created_at, updated_at, source_out))
        out_txn_id = cursor.fetchone()[0]

        # cursor.execute("""
        #     INSERT INTO entries (
        #         account_id, entryable_type, entryable_id, amount, currency, date, name,
        #         created_at, updated_at, notes, locked_attributes, external_id, source
        #     )
        #     VALUES (
        #         %s, 'Transaction', %s, %s, 'INR', %s, %s,
        #         %s, NOW(), %s, '{}'::jsonb, %s, %s
        #     )
        #     RETURNING id
        # """, (
        #     from_account, out_txn_id, amount, date, name,
        #     created_at, updated_at, notes, external_id, source_out
        # ))
        cursor.execute("""
            INSERT INTO entries (
                account_id, entryable_type, entryable_id, amount, currency, date, name,
                created_at, updated_at, notes, locked_attributes, external_id, source
            )
            VALUES (
                %s, 'Transaction', %s, %s, 'INR', %s, %s,
                %s, NOW(), %s, '{}'::jsonb, %s, %s
            )
            RETURNING id
        """, (
            from_account, out_txn_id, abs(amount), date, 'Transfer to ' + to_account_name,
            created_at, notes, external_id, source_out
        ))


        # --- Inflow Transaction (credit) ---
        # cursor.execute("""
        #     INSERT INTO transactions (created_at, updated_at, kind, external_id)
        #     VALUES (%s, NOW(), 'funds_movement', %s)
        #     RETURNING id
        # """, (created_at, updated_at, source_in))
        # in_txn_id = cursor.fetchone()[0]
        #
        # cursor.execute("""
        #     INSERT INTO entries (
        #         account_id, entryable_type, entryable_id, amount, currency, date, name,
        #         created_at, updated_at, notes, locked_attributes, external_id, source
        #     )
        #     VALUES (
        #         %s, 'Transaction', %s, %s, 'INR', %s, %s,
        #         %s, NOW(), %s, '{}'::jsonb, %s, %s
        #     )
        #     RETURNING id
        # """, (
        #     to_account, in_txn_id, amount, date, name,
        #     created_at, updated_at, notes, external_id, source_in
        # ))
        #
        # # --- Transfer linking ---
        # cursor.execute("""
        #     INSERT INTO transfers (outflow_transaction_id, inflow_transaction_id, status, created_at, updated_at)
        #     VALUES (%s, %s, %s, %s, %s)
        # """, (out_txn_id, in_txn_id, 'confirmed', created_at, NOW()))

        # import ipdb
        # ipdb.set_trace()
        # --- Inflow Transaction (credit) ---
        # Use both created_at and updated_at as parameters (keeps behaviour consistent)
        cursor.execute("""
            INSERT INTO transactions (created_at, updated_at, kind, external_id)
            VALUES (%s, %s, 'funds_movement', %s)
            RETURNING id
        """, (created_at, updated_at, source_in))
        in_txn_id = cursor.fetchone()[0]

        # --- Inflow entry (credit) ---
        # Note placeholders count matches tuple length below
        cursor.execute("""
            INSERT INTO entries (
                account_id, entryable_type, entryable_id, amount, currency, date, name,
                created_at, updated_at, notes, locked_attributes, external_id, source
            )
            VALUES (
                %s, 'Transaction', %s, %s, 'INR', %s, %s,
                %s, %s, %s, '{}', %s, %s
            )
            RETURNING id
        """, (
            to_account,      # account_id
            in_txn_id,       # entryable_id
            -amount,          # amount (positive)
            date,            # date
            'Transfer from ' + from_account_name,            # name
            created_at,      # created_at
            updated_at,      # updated_at  <-- replaced NOW() with placeholder
            notes,           # notes
            external_id,     # external_id (UTR)
            source_in        # source
        ))

        # --- Transfer linking ---
        # Use placeholders for both created_at and updated_at (no SQL function in params)
        cursor.execute("""
            INSERT INTO transfers (outflow_transaction_id, inflow_transaction_id, status, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (out_txn_id, in_txn_id, 'confirmed', created_at, updated_at))


        cursor.execute("COMMIT;")

        return ("created", out_txn_id, in_txn_id)

    except Exception as e:
        cursor.execute("ROLLBACK;")
        return ("error", str(e))

def insert_transactions(conn, records, min_date=None, dry_run=False):
    inserted = 0
    to_write = []
    cur = conn.cursor()
    try:
        for r in records:
            # date check
            try:
                txn_date = datetime.strptime(r['date'], "%Y-%m-%d").date()
            except Exception:
                logging.warning("Invalid date, skipping: %s", r)
                continue
            if min_date and txn_date < min_date:
                continue
            # choose account_id (self account is destination for entries)
            self_account = os.getenv("SURE_SELF_ACCOUNT_ID") or DEFAULT_SELF_ACCOUNT_ID
            # if mapped account (transfer) skip for now - you can change this behaviour
            # account_id_from_map = find_account_id(r.get("name"))
            # if account_id_from_map:
            #     logging.info("Skipping transfer to mapped account %s", account_id_from_map)
            #     continue
            # existence check (prefer checking with account_id if available)
            exists = txn_exists(conn, r.get("transaction_id"), r.get("utr_no"), self_account)
            if exists:
                logging.info("Already exists, skipping: source=%s external_id=%s", r.get("transaction_id"), r.get("utr_no"))
                continue
            amt_dec = safe_decimal(r.get("amount"))
            if amt_dec is None:
                logging.warning("Invalid amount, skipping: %s", r)
                continue
            # prepare row
            row = {
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
                "date": r["date"],
                "name": r.get("name") or "PhonePe",
                "amount": amt_dec,  # Decimal quantized to 4 decimals
                "external_id": r.get("utr_no") or None,
                "source": r.get("transaction_id") or None
            }
            if dry_run:
                to_write.append({**row, "source": row["source"], "external_id": row["external_id"]})
                continue
            try:
                is_transfer = perform_transfer(conn, r, accounts)

                if is_transfer[0] == "created":
                    print("Transfer inserted:", is_transfer)
                    continue

                elif is_transfer[0] == "exists":
                    print("Transfer already exists:", is_transfer)
                    continue

                elif is_transfer[0] == "skip":
                    print("Detected as normal expense")
                    continue
                    # not a transfer → let normal importer handle

                    # handle as normal expense (not a transfer)
                    cur.execute("""
                        INSERT INTO transactions (created_at, updated_at, category_id, merchant_id, locked_attributes, kind, external_id)
                        VALUES (%s, %s, NULL, NULL, '{}', 'standard', %s)
                        RETURNING id
                    """, (row["created_at"], NOW(), row["source"]))
                    trans_id = cur.fetchone()[0]
                    # insert entry: must supply account_id (not null) and name (not null)
                    cur.execute("""
                        INSERT INTO entries (
                            account_id, entryable_type, entryable_id, amount, currency, date, name,
                            created_at, updated_at, import_id, notes, excluded, plaid_id, locked_attributes, external_id, source
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, false, NULL, '{}', %s, %s)
                        RETURNING id
                    """, (
                        self_account, 'Transaction', trans_id,
                        row["amount"], 'INR', row["date"], row["name"],
                        row["created_at"], NOW(), 'Added via automation-script', row["external_id"], row["source"]
                    ))
                    entry_id = cur.fetchone()[0]
                    conn.commit()
                    inserted += 1
                    logging.info("Inserted entry id=%s trans=%s amount=%s", entry_id, trans_id, row["amount"])
                else:
                    logging.error("Transfer failed: %s", row)
                # accounts = your list of dicts (from config)
                # insert transactions row, set external_id to source (optional) so you can refer later
            except Exception:
                conn.rollback()
                logging.exception("Failed to insert row: %s", row)
                continue
    finally:
        cur.close()
    # dry-run write
    if dry_run and to_write:
        out_path = Path("phonepe_parsed_dryrun.csv")
        with out_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(to_write[0].keys()))
            writer.writeheader()
            writer.writerows(to_write)
        logging.info("Dry-run CSV written to %s (%d rows)", out_path, len(to_write))
    logging.info("Done. Inserted %d new rows", inserted)
    return inserted

def main():
    if len(sys.argv) < 2:
        print("Usage: python phonepe_expense_update_tuned_v2.py input.pdf|input.txt [--min-date=YYYY-MM-DD] [--dry-run]")
        return
    inp = Path(sys.argv[1])
    min_date = None
    dry_run = False
    for a in sys.argv[2:]:
        if a.startswith("--min-date"):
            try:
                min_date = datetime.strptime(a.split("=",1)[1], "%Y-%m-%d").date()
            except Exception:
                logging.error("Invalid --min-date value, expected YYYY-MM-DD")
                return
        elif a == "--dry-run":
            dry_run = True
        else:
            logging.warning("Unknown argument passed: %s", a)
    if inp.suffix.lower() == ".pdf":
        pdf_to_parse = decrypt_pdf_if_needed(inp)
        tmp = Path(tempfile.NamedTemporaryFile(delete=False, suffix=".txt").name)
        try:
            run_pdf2txt(pdf_to_parse, tmp)
            records = parse_txt_file(tmp)
        finally:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
    else:
        records = parse_txt_file(inp)
    logging.info("Parsed %d records", len(records))
    conn = connect_to_postgres()
    if not conn:
        logging.error("DB connection failed; abort")
        return
    try:
        inserted = insert_transactions(conn, records, min_date=min_date, dry_run=dry_run)
        logging.info("Inserted %d rows", inserted)
    finally:
        conn.close()

if __name__ == "__main__":
    main()

