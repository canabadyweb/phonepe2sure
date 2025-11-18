import sys
import os
import re
import json
import base64
import email
import psycopg2
from datetime import datetime
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


def get_gmail_service():
    """Authenticate and return Gmail API service."""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return build('gmail', 'v1', credentials=creds)


def connect_to_postgres():
    """Establish and return a PostgreSQL connection."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None


def get_last_transaction_date(conn):
    """Fetch the last transaction date from the database to filter new emails."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(created_at) FROM account_entries;")
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


def convert_to_date_format(date_str):
    # Define possible date formats
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",     # Matches "Sun, 09 Mar 2025 13:06:31 +0530"
        # Matches "Sun, 9 Mar 2025 09:54:18 +0530 (IST)"
        "%a, %d %b %Y %H:%M:%S %z (%Z)",
        "%a, %d %b %Y %H:%M:%S %z"       # Handle single-digit day variations
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt
            # return dt.strftime("%Y-%m-%d %H:%M:%S.%f")  # Convert to YYYY-MM-DD
        except ValueError:
            continue

        raise ValueError(f"Date format not recognized: {date_str}")


def fetch_phonepe_emails(conn):
    """Fetch PhonePe transaction emails after the last transaction date."""
    service = get_gmail_service()
    last_date_timestamp = get_last_transaction_date(conn)

    query = 'from:noreply@phonepe.com subject:"Sent"'
    if last_date_timestamp:
        query += f" after:{last_date_timestamp}"

    results = service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])

    transactions = []
    for msg in messages:
        msg_data = service.users().messages().get(
            userId='me', id=msg['id']).execute()
        headers = msg_data['payload']['headers']

        # Extract subject and date
        subject = next(h['value']
                       for h in headers if h['name'].lower() == 'subject')
        date_str = next(h['value']
                        for h in headers if h['name'].lower() == 'date')

        # Convert email date format
        # date_obj = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
        date_obj = convert_to_date_format(date_str)
        txn_date = date_obj.strftime("%Y-%m-%d")
        created_at = date_obj.strftime("%Y-%m-%d %H:%M:%S.%f")

        # Extract amount and merchant name from subject
        match = re.search(r"Sent\s*₹\s*([\d.]+)\s*to\s*(.*)", subject)
        if match:
            amount = match.group(1)
            name = match.group(2).strip()

            txn = {
                "date": txn_date,
                "name": name,
                "amount": round(float(amount), 4),
                "created_at": created_at,
                "updated_at": created_at
            }
            transactions.append(txn)

    return sorted(transactions, key=lambda x: x["created_at"])


def insert_into_postgres(conn, transactions):
    """Insert transaction data into PostgreSQL."""
    with conn.cursor() as cur:
        for txn in transactions:
            # Insert into account_transactions
            cur.execute("""
                INSERT INTO account_transactions ("created_at", "updated_at")
                VALUES (%s, %s) RETURNING id
            """, (txn['created_at'], txn['updated_at']))
            entryable_id = cur.fetchone()[0]

            account_id = '54f3d108-9ed2-446c-a489-ed1c2ffdf5b0'

            # Insert into account_entries
            cur.execute("""
                INSERT INTO account_entries (
                    account_id, entryable_type, entryable_id, amount, currency, date, name,
                    created_at, updated_at, import_id, notes, excluded, plaid_id, enriched_at, enriched_name
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, 'Added via automation-script', false, NULL, NULL, NULL)
            """, (account_id, 'Account::Transaction', entryable_id, txn['amount'], 'INR', txn['date'], txn['name'], txn['created_at'], txn['updated_at']))

            # # Update accounts table
            # cur.execute("""
            #     UPDATE accounts
            #     SET balance = balance - %s, cash_balance = cash_balance - %s
            #     WHERE id = %s RETURNING id
            # """, (txn['amount'], txn['amount'], account_id))

            # updated_id = cur.fetchone()
            # if updated_id:
            #     print(f"Updated accounts for transaction {updated_id[0]}")

            # # Update account_balances table
            # cur.execute("""
            #     UPDATE account_balances
            #     SET balance = balance - %s, cash_balance = cash_balance - %s
            #     WHERE account_id = %s AND date = %s RETURNING id
            # """, (txn['amount'], txn['amount'], account_id, datetime.today().strftime('%Y-%m-%d')))

            # updated_id = cur.fetchone()
            # if updated_id:
            #     print(
            #         f"Updated account_balances for transaction {updated_id[0]}")

        conn.commit()


def main():
    """Main automation flow."""
    conn = connect_to_postgres()
    if not conn:
        return

    transactions = fetch_phonepe_emails(conn)
    if transactions:
        # print(transactions)
        insert_into_postgres(conn, transactions)
        print(f"Inserted {len(transactions)} transactions into PostgreSQL.")
    else:
        print("No new transactions found.")

    conn.close()


if __name__ == "__main__":
    main()
