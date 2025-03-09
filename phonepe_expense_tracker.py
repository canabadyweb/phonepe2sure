import os
import re
import json
import base64
import email
from datetime import datetime
import psycopg2

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# Expense Categories
categories = {
    "none": "",
    "Auto": "71ea074a-1d8e-4454-bd67-0e99eae7dde8",
    "Breakfast": "2f266956-fd6d-44f8-8e8a-6f4d91c301c5",
    "Broadband": "16fcec11-1af0-4ea5-b23c-b409c2881d2c",
    "Bus": "16f5ffd4-d81c-4f74-9080-affbcf8e8b75",
    "Cloudtree AC": "514a9739-0588-4246-a52b-527c924f532d",
    "Cosmetic": "940e9dfa-7a72-4c01-b8e5-8828cb62417a",
    "Dad AC": "fc3778d8-f015-4171-95b1-ff41a6c47b3a",
    "Delivery Fee": "7c02a670-000c-4b3a-bbed-fd6072fdf613",
    "Dining Out": "154171ac-fead-47b6-b372-9c469f8c0201",
    "Dinner": "c05eccc6-a42d-41c0-8395-847c0b21515e",
    "Dress Ironing": "a320eb81-b986-4d46-8961-f0c478afb04c",
    "Education": "6a0a494f-ec00-4ab9-b69a-232b8657ae35",
    "Electricity": "e021c4c7-a19f-42de-b050-f977f3e0d891",
    "Entertainment": "f0ab84d8-e48a-42a1-bd8e-3b19be8c591d",
    "Fish": "b06bc0eb-2346-47b7-bd07-ca986aa64ed4",
    "Flower": "fcb7baba-46c6-463f-93e2-07e3519f5cf4",
    "Food": "72a2ed39-9ad1-47c5-a3d5-989a50a77430",
    "Food Delivery": "77f5aeb8-f988-42d4-9b1f-a5ba5185c1d7",
    "From PSK": "20553513-805b-43b3-9c1f-f72ebef03c5d",
    "Fruit": "737c90f8-c27b-4c56-a337-327f9299abd9",
    "Gifts & Donations": "b6014386-7a9e-4b1a-b5a5-3b36789f151f",
    "Grocery": "4299b0ce-dc2c-437d-ac1e-ee3c997beb4b",
    "Healthcare": "872513e6-acfe-434e-922e-d370663945e3",
    "House-Rent": "e7d8d7f6-1915-4339-9618-9f221ac1c84d",
    "Idly Maavu": "e7bf9d1e-5e79-4942-83cf-62af47be4e4d",
    "Income": "b64081c1-0480-4880-9983-bb352776e189",
    "Loyola College": "1ce54c97-d869-4121-8684-e10e99621b85",
    "Lunch": "9117ab8c-466d-4b5c-a348-929c154271e3",
    "Medical": "b54d1591-86b3-4a63-ad12-f0445888b6b4",
    "Metro": "db71dbfc-ea03-42d4-905f-76d92cb8445d",
    "Milk": "04ea9fdf-6bdf-41ff-b8b8-7e9b66bee6ae",
    "Mutton": "bbddf938-bccf-47dc-b93e-dca058729b0b",
    "Product Delivery": "86bb96df-fda8-474d-b1af-2a699f1f3171",
    "PSK AC": "07ef84f1-52c8-4bdc-aea9-16d31b4d951c",
    "PSS AC": "20ca5908-94d5-47b6-bac7-a4d40dd2c582",
    "Refreshment": "e276b23c-4427-44ba-b1a8-d82049435de4",
    "Shopping": "9c88bc64-dc5f-485e-9d75-4a28841fb98a",
    "Stationery": "d39e3a74-e4f8-4bb8-a724-41e74eaaa07e",
    "Subscriptions": "8555fd45-9ef6-4a50-8f70-6bdc83e1c74c",
    "Sweets and Snacks": "4e505694-0f70-499f-98ea-2fbdee01d37c",
    "Taxi": "91566d5a-2355-422e-b7ae-0503ab34bf15",
    "Train": "2afde446-bdfa-462d-bbf1-e9931f99f388",
    "Transportation": "b15b9448-18d3-412b-9211-d3e4990fe11d",
    "Utensil": "686a3423-5655-4698-8b92-98e53573157b",
    "Utilities": "acdae209-cafd-4bc2-8e4d-9a3c196179b4",
    "Vegetable": "81144fa6-d069-4430-83fa-0d5b903faf91",
    "Water Can": "bc6b1864-e3a0-46dd-860c-3b566f818a82"
}

merchants = {
    "(none)": "",
    "Aasife Biriyani, Butt Road, Chennai": "a7040e75-eb6b-4fbe-b437-8a0dddba6392",
    "Ayyapan Tiffin Center": "96b5f0fc-bcd4-4a19-8d0d-1c7da75971b9",
    "Dhakshinamurthy (Dhatchu Kitchen)": "cdaaf310-cb57-4c12-9ce3-ec241bf6841f",
    "Ganga Sweets": "9101d883-60b9-4267-89bc-d5129fcadf46",
    "Grace Supermarket": "9d45237e-0042-4b89-ba74-0603505c2af3",
    "Hotel Varasha": "270b20da-534e-47a0-853f-31f88347310b",
    "INFIBEAM AVENUES LIMITED": "7fba0d64-aa08-4b3c-a04d-f6808a766bf0",
    "MADHUS KKITCHEN": "ad1241bd-9e62-4749-993d-c16b238aa5c6",
    "MAGIZH VEGETABLE AND": "83a37cdf-6b98-4ceb-a44c-3a00bb841c44",
    "MASTHANAIAH ACHALA": "c3e77a02-4858-4b81-ac55-f16b9355ae40",
    "Mr MUTHUMANI K": "60f0108d-d67c-45f8-8a6c-0dfbb10c1da0",
    "NANBAN FISH STALL": "d22a4062-936c-4823-8d2a-bad2f057a602",
    "Ravi": "298fa732-41af-444d-8d1e-cf3cf0a7ddad",
    "RAVIKUMAR R": "cd0009ba-7f49-4445-abe1-c93c54dc2828",
    "SHRINICAS DEPARTMENTAL STORES": "eda57643-d8c8-4d2d-9d6e-90941b05d85f",
    "SRI MOOGAMBIGAI MEDICALS": "bd0cd6aa-35b9-4993-83ee-8ac49a041f90"
}

merchant_categories = {
    "Aasife Biriyani, Butt Road, Chennai": ["Lunch", "Dining Out"],
    "Ayyapan Tiffin Center": ["Breakfast", "Dining Out"],
    "Dhakshinamurthy (Dhatchu Kitchen)": ["Lunch", "Dinner"],
    "Ganga Sweets": ["Sweets and Snacks", "Refreshment"],
    "Grace Supermarket": ["Grocery", "Vegetable", "Cosmetic"],
    "Hotel Varasha": ["Breakfast", "Dinner", "Lunch", "Dining Out"],
    "INFIBEAM AVENUES LIMITED": ["Water Can", "Online Shopping", "Subscriptions"],
    "MADHUS KKITCHEN": ["Lunch", "Dinner"],
    "MAGIZH VEGETABLE AND": ["Milk", "Fruit", "Vegetable", "Grocery"],
    "MASTHANAIAH ACHALA": ["Idly Maavu", "Food"],
    "Mr MUTHUMANI K": ["Water Can", "Milk", "Grocery"],
    "NANBAN FISH STALL": ["Fish", "Food"],
    "Ravi": ["Flower"],
    "RAVIKUMAR R": ["Flower"],
    "SHRINICAS DEPARTMENTAL STORES": ["Vegetable", "Grocery", "Cosmetic"],
    "SRI MOOGAMBIGAI MEDICALS": ["Medical", "Healthcare"]
}


def convert_to_date_format(date_str, date_format):
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
            return dt.strftime(date_format)  # Convert to YYYY-MM-DD
        except ValueError:
            continue

    raise ValueError(f"Date format not recognized: {date_str}")


def authorize_gmail():
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


def get_gmail_service():
    creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    return build('gmail', 'v1', credentials=creds)


def fetch_phonepe_emails(connection):
    service = get_gmail_service()

    cursor = connection.cursor()
    query = 'from:noreply@phonepe.com subject:"Sent" after:{}'.format(
        after_timestamp(cursor))
    cursor.close()

    results = service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])

    transactions = []

    for msg in messages:
        msg_data = service.users().messages().get(
            userId='me', id=msg['id']).execute()

        # # Handle multipart emails
        # parts = msg_data['payload'].get('parts', [])
        # body = ""
        # for part in parts:
        #     if part['mimeType'] == 'text/plain':
        #         body = base64.urlsafe_b64decode(
        #             part['body']['data']).decode('utf-8')
        #         break

        # if not body:
        #     continue

        headers = msg_data['payload']['headers']

        # Extract Subject and Date from headers
        subject = next(h['value']
                       for h in headers if h['name'].lower() == 'subject')
        date_str = next(h['value']
                        for h in headers if h['name'].lower() == 'date')

        # dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
        # date = dt.strftime("%Y-%m-%d")
        date = convert_to_date_format(date_str, "%Y-%m-%d")
        created_at = convert_to_date_format(date_str, "%Y-%m-%d %H:%M:%S.%f")

        # Parse transaction details from email subject
        pattern = r"Sent\s*₹\s*([\d.]+)\s*to\s*(.*)"
        match = re.search(pattern, subject)

        if match:
            amount = match.group(1)
            name = match.group(2).strip()

            category_id = None
            merchant_id = None
            if name in merchants:
                merchant_id = merchants[name]
                category_id = categories[merchant_categories[name][0]]

        txn = {
            "date": date,
            "name": name,
            "amount": '{:.4f}'.format(float(amount)),
            "category_id": category_id,
            "merchant_id": merchant_id,
            "created_at": created_at,
            "updated_at": created_at,
        }
        print(txn)

        transactions.append(txn)

    # Sort transactions by date in ascending order
    transactions.sort(key=lambda x: x["created_at"])

    return transactions


def connect_to_postgres():
    # Access DB environment variables
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    return conn


def after_timestamp(connection):

    cursor = connection.cursor()
    # Run a SELECT query
    cursor.execute("select * from account_entries order by date desc limit 1;")
    record = cursor.fetchone()

    date = record[8]

    # Convert datetime to UNIX timestamp (required for precise filtering)
    after_timestamp = int(date.timestamp())

    return after_timestamp


def insert_into_postgres(connection, transactions):

    cursor = connection.cursor()

    for txn in transactions:
        cursor.execute("""
            INSERT INTO account_transactions ("created_at", "updated_at", "category_id", "merchant_id")
            VALUES (%s, %s, %s, %s) RETURNING "id"
        """, (txn['created_at'], now(), txn['category_id'], txn['merchant_id']))

        entryable_id = cur.fetchone()[0]  # Fetch the returned id
        entryable_type = 'Account::Transaction',
        account_id = '54f3d108-9ed2-446c-a489-ed1c2ffdf5b0'

        cursor.execute("""

            INSERT INTO account_entries (
                account_id,
                entryable_type,
                entryable_id,
                amount,
                currency,
                date,
                name,
                created_at,
                updated_at,
                import_id,
                notes,
                excluded,
                plaid_id,
                enriched_at,
                enriched_name
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (account_id,
              'Account::Transaction',
              entryable_id,
              txn['amount'], 'INR', txn['date'],
              txn['name'], txn['created_at'], now(), NULL,
              'Added via automation-script',
              false,
              NULL, NULL, NULL
              ))

        # Execute the UPDATE statement
        cur.execute("""
            UPDATE accounts
            SET
            balance = %s,
            cash_balance = %s
            WHERE id = %s
            RETURNING id
        """, (balance-txn['amount'], cash_balance-txn['amount'], account_id))

        updated_id = cur.fetchone()

        if updated_id:
            print(f"Transaction ID {updated_id[0]} updated successfully.")
        else:
            print("accounts table is NOT record updated.")

        cur.execute("""
            UPDATE account_balances
            SET
            balance = %s,
            cash_balance = %s
            WHERE account_id = %s AND date = %s
            RETURNING id
        """, (balance-txn['amount'], cash_balance-txn['amount'],
              account_id, datetime.today().strftime('%Y-%m-%d')))

        updated_id = cur.fetchone()

        if updated_id:
            print(f"Transaction ID {updated_id[0]} updated successfully.")
        else:
            print("accounts table is NOT record updated.")

    connection.commit()
    cursor.close()
    connection.close()


# Run Flow
connection = connect_to_postgres()
authorize_gmail()
transactions = fetch_phonepe_emails(connection)


if transactions:
    insert_into_postgres(cursor, transactions)
    print(
        f"Inserted {len(transactions)} transactions into PostgreSQL inside Docker.")
else:
    print("No new transactions found.")
