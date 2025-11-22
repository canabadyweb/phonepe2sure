#!/usr/bin/env python3
"""
phonepe_expense_update_cleaned.py

Cleaner, safer version of your PhonePe -> Sure DB importer.
Based on your uploaded script. See original for more context. :contentReference[oaicite:1]{index=1}

Usage:
  pip install python-dotenv psycopg2-binary PyPDF2 pdfminer.six
  python phonepe_expense_update_cleaned.py input.pdf|input.txt [--min-date=YYYY-MM-DD] [--dry-run]
"""

from __future__ import annotations

import csv
import getpass
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
from typing import Dict, List, Optional, Tuple

import psycopg2
from dotenv import load_dotenv
from PyPDF2 import PdfReader, PdfWriter

# Decimal precision
getcontext().prec = 28

# Logging (file + console)
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

# Regexes (kept from original)
DATE_FIND_RE = re.compile(
    r'([A-Za-z]{3,9}\s*\d{1,2},\s*\d{4}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}-\d{1,2}-\d{1,2})'
)
TIME_FIND_RE = re.compile(r'(\d{1,2}:\d{2}\s*(?:AM|PM))', re.IGNORECASE)
INR_AMT_RE = re.compile(r'INR\s*([0-9,]+(?:\.[0-9]+)?)', re.IGNORECASE)
AMOUNT_RE = re.compile(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)|\d+(?:\.[0-9]+)?)')
TXN_ID_RE = re.compile(r'Transaction\s*ID\s*[:\-\s]*([A-Za-z0-9]+)', re.IGNORECASE)
UTR_RE = re.compile(r'UTR\s*No\s*[:\-\s]*([A-Za-z0-9]+)', re.IGNORECASE)
DEBIT_WORD_RE = re.compile(r'\bDebit\b', re.IGNORECASE)
CREDIT_WORD_RE = re.compile(r'\bCredit\b', re.IGNORECASE)
HEADER_KEYWORDS = ["date transaction details", "transaction details", "date transaction details type amount"]

DEFAULT_SELF_ACCOUNT_ID = os.getenv("SURE_SELF_ACCOUNT_ID", "54f3d108-9ed2-446c-a489-ed1c2ffdf5b0")

# keep your account map (unchanged)
ACCOUNTS = [
    {"account_name": "PSG SBI AC", "account_id": "54f3d108-9ed2-446c-a489-ed1c2ffdf5b0", "transaction_name": "SELF"},
    {"account_name": "PSM AC", "account_id": "927a3ee1-3963-4ebf-97cd-137910e155c3", "transaction_name": "P SELVAM"},
    {"account_name": "PSK SBI AC", "account_id": "2afe1683-e05d-442f-ab1b-159d92d1b34e", "transaction_name": "SELVAKUMARAN P"},
    {"account_name": "PSS SBI AC", "account_id": "f7b7839c-1fb4-429c-8088-441bf01a14a7", "transaction_name": "SELVASANKAR P"},
    {"account_name": "Dad SBI AC", "account_id": "7b58df97-381d-45a4-882e-d8364f793fbc", "transaction_name": "PERUMAL R"},
    {"account_name": "Cloudtree AC", "account_id": "ca529321-4bf8-430c-aab5-90c333ca34a3", "transaction_name": "CLOUDTREE"},
    {"account_name": "PhonePe Wallet", "account_id": "340ca703-1057-4ce9-a038-730a26d20aea", "transaction_name": "PhonePe Wallet"},
]


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


def parse_pdf2txt_lines(lines: List[str]) -> List[Dict]:
    records = []
    i = 0
    n = len(lines)
    while i < n:
        ln = lines[i].strip()
        m = DATE_FIND_RE.search(ln)
        if not m:
            i += 1
            continue
        date_token = m.group(1).strip()

        # find optional time in following lines (skip blank)
        time_token = ""
        j = i + 1
        while j < n and not lines[j].strip():
            j += 1
        if j < n:
            mt = TIME_FIND_RE.search(lines[j].strip())
            if mt:
                time_token = mt.group(1).strip()
                j += 1

        # collect block until next date token
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

        # normalize numeric string
        amount_val = None
        if amount_txt:
            try:
                amount_val = f"{float(amount_txt):.2f}"
            except Exception:
                amount_val = amount_txt

        # sign convention: debit → negative
        if txn_type and txn_type.lower() == "debit" and amount_val:
            try:
                amount_val = f"-{abs(float(amount_val)):.2f}"
            except Exception:
                pass

        date_norm = normalize_date(date_token)
        time_norm = normalize_time(time_token)
        ts_time = time_norm or "00:00"
        try:
            created_dt = datetime.strptime(f"{date_norm} {ts_time}", "%Y-%m-%d %H:%M")
            created_at = created_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        except Exception:
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        if paid_to and paid_to.lower().startswith("date transaction details"):
            continue

        records.append(
            {
                "date": date_norm,
                "time": time_norm,
                "created_at": created_at,
                "updated_at": updated_at,
                "name": paid_to or "PhonePe",
                "transaction_id": txn_id or None,
                "utr_no": utr or None,
                "type": txn_type or None,
                "amount": amount_val,
            }
        )

    return records


def parse_txt_file(path: Path) -> List[Dict]:
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
        logger.error("Missing DB configuration in environment. Please set SURE_DB_NAME, SURE_DB_USER, SURE_DB_PASSWORD")
        return None
    try:
        conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)
        return conn
    except Exception:
        logger.exception("Database connection failed")
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


def find_account_match_by_name(name: str):
    if not name:
        return None
    nl = name.lower()
    for acc in ACCOUNTS:
        if acc["transaction_name"].lower() in nl:
            return acc
    return None


def perform_transfer(conn, txn: Dict, accounts: List[Dict]) -> Tuple[str, Optional[int], Optional[int]]:
    """
    Returns:
      ("created", outflow_txn_id, inflow_txn_id)
      ("exists", outflow_txn_id, inflow_txn_id)
      ("skip", reason, None)
      ("error", error_msg, None)
    """
    name = (txn.get("name") or "").strip()
    matched = find_account_match_by_name(name)
    if not matched:
        return ("skip", None, None)

    to_account = matched["account_id"]
    to_name = matched["account_name"]
    from_account = DEFAULT_SELF_ACCOUNT_ID
    from_name = "SELF_ACCOUNT"

    # parse amount as Decimal
    amt = safe_decimal(txn.get("amount"))
    if amt is None:
        return ("skip", None, None)

    # if positive amount means money came IN to self, flip from/to
    if amt > Decimal("0"):
        # treat as incoming -> swap
        tmp_id, tmp_name = to_account, to_name
        to_account, to_name = from_account, from_name
        from_account, from_name = tmp_id, tmp_name

    source_out = txn.get("transaction_id") or f"PHONEPE-{txn.get('created_at')}"
    source_in = f"{source_out}_IN"
    external_id = txn.get("utr_no")

    # idempotency: if transfers already present by source_out OR both external ids
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

            # begin atomic
            cur.execute("BEGIN;")

            # outflow transaction
            cur.execute(
                "INSERT INTO transactions (created_at, updated_at, kind, external_id) VALUES (%s, %s, 'funds_movement', %s) RETURNING id",
                (txn["created_at"], txn["updated_at"], source_out),
            )
            out_txn_id = cur.fetchone()[0]

            # outflow entry: amount should be negative for outflow
            out_amount = abs(amt)
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

            # inflow transaction
            cur.execute(
                "INSERT INTO transactions (created_at, updated_at, kind, external_id) VALUES (%s, %s, 'funds_movement', %s) RETURNING id",
                (txn["created_at"], txn["updated_at"], source_in),
            )
            in_txn_id = cur.fetchone()[0]

            # inflow entry (positive)
            in_amount = -abs(amt)
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

            # link transfer
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
    self_account = os.getenv("SURE_SELF_ACCOUNT_ID") or DEFAULT_SELF_ACCOUNT_ID
    print(records)

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

            exists = txn_exists(conn, r.get("transaction_id"), r.get("utr_no"), self_account)
            if exists:
                logger.info("Already exists, skipping: source=%s external_id=%s", r.get("transaction_id"), r.get("utr_no"))
                continue

            amt_dec = safe_decimal(r.get("amount"))
            if amt_dec is None:
                logger.warning("Invalid amount, skipping: %s", r)
                continue

            # First, try treating it as a transfer (internal account match)
            transfer_result = perform_transfer(conn, r, ACCOUNTS)
            if transfer_result[0] == "created":
                logger.info("Inserted transfer for source=%s", r.get("transaction_id"))
                continue
            elif transfer_result[0] == "exists":
                logger.info("Transfer exists for source=%s", r.get("transaction_id"))
                continue
            elif transfer_result[0] == "error":
                logger.error("Transfer error, will try as expense: %s", transfer_result[1])
                # fall through to expense handling

            # Normal expense insertion path
            row = {
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
                "date": r["date"],
                "name": r.get("name") or "PhonePe",
                "amount": -amt_dec,
                "external_id": r.get("utr_no"),
                "source": r.get("transaction_id"),
            }

            if dry_run:
                dry_rows.append(row)
                continue

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO transactions (created_at, updated_at, category_id, merchant_id, locked_attributes, kind, external_id)
                        VALUES (%s, %s, NULL, NULL, '{}'::jsonb, 'standard', %s)
                        RETURNING id
                        """,
                        (row["created_at"], row["updated_at"], row["source"]),
                    )
                    trans_id = cur.fetchone()[0]

                    cur.execute(
                        """
                        INSERT INTO entries (
                            account_id, entryable_type, entryable_id, amount, currency, date, name,
                            created_at, updated_at, import_id, notes, excluded, plaid_id, locked_attributes, external_id, source
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, false, NULL, '{}'::jsonb, %s, %s)
                        RETURNING id
                        """,
                        (
                            self_account,
                            "Transaction",
                            trans_id,
                            row["amount"],
                            "INR",
                            row["date"],
                            row["name"],
                            row["created_at"],
                            row["updated_at"],
                            "Added via automation-script",
                            row["external_id"],
                            row["source"],
                        ),
                    )
                    entry_id = cur.fetchone()[0]
                    conn.commit()
                    inserted += 1
                    logger.info("Inserted expense entry id=%s trans=%s amount=%s", entry_id, trans_id, row["amount"])
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logger.exception("Failed to insert expense row: %s", row)
                continue
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
        print("Usage: python phonepe_expense_update_cleaned.py input.pdf|input.txt [--min-date=YYYY-MM-DD] [--dry-run]")
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

    logger.info("Parsed %d records", len(records))
    conn = connect_to_postgres()
    if not conn:
        logger.error("DB connection failed; abort")
        return

    try:
        inserted = insert_transactions(conn, records, min_date=min_date, dry_run=dry_run)
        logger.info("Inserted %d rows", inserted)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
