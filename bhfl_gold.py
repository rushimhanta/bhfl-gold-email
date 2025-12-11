import os
import io
import tempfile
from datetime import datetime
import boto3
import pandas as pd
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak, Frame, PageTemplate
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from PyPDF2 import PdfReader, PdfWriter
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

# ---------------- CONFIG ----------------
S3_BUCKET = "bhfl-bank-transformed"
BASE_PATH = "transactions"                 # base prefix
AWS_REGION = "ap-south-1"
SENDER_EMAIL = "rushikeshmhanta@gmail.com"    # must be verified in SES if sandbox
OUTPUT_S3_PREFIX = "emails-data"            # optional prefix to upload protected pdfs
SEND_VIA_SES = True
UPLOAD_TO_S3 = True
SEND_SMS_PASSWORD = True                   # sends password via SNS to phone (recommended)
LOGO_S3_KEY = None                         # e.g., "assets/logo.png" or None to skip logo
FONT_TTF_PATH = None                       # optional custom font path (ttf). None uses defaults.
# ----------------------------------------

s3 = boto3.client("s3", region_name=AWS_REGION)
ses = boto3.client("ses", region_name=AWS_REGION) if SEND_VIA_SES else None

styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name='Small', fontSize=9, leading=11))
styles.add(ParagraphStyle(name='Tiny', fontSize=8, leading=10))

PAGE_SIZE = LETTER
PAGE_WIDTH, PAGE_HEIGHT = PAGE_SIZE

# Map your parquet columns here (change if your names differ)
COLS = {
    "cust_id": "customer_id",            # comes from folder name also
    "name": "first_name",                # you can combine first_name + last_name later
    "email": "email_id",
    "phone": "phone_no",
    "acct": "account_id",
    "date": "transaction_date",
    "desc": "description",
    "amt": "amount",
    "bal": "availablebalance",
}


# -------- helpers --------
def list_customer_folders(month):
    """Return list of cust_id values under BASE_PATH/month={month}/"""
    prefix = f"{BASE_PATH}/month={month}/"
    paginator = s3.get_paginator("list_objects_v2")
    customer_ids = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            p = cp["Prefix"]
            if "cust_id=" in p:
                cust = p.split("cust_id=")[1].rstrip("/")
                customer_ids.append(cust)
    return customer_ids

def list_parquet_keys_for_customer(month, cust_id):
    prefix = f"{BASE_PATH}/month={month}/cust_id={cust_id}/"
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(".parquet"):
                keys.append(obj["Key"])
    return keys

def load_parquet_from_s3(key):
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        s3.download_file(S3_BUCKET, key, tmp.name)
        df = pd.read_parquet(tmp.name)
    os.unlink(tmp.name)
    return df

def assemble_customer_df(month, cust_id):
    keys = list_parquet_keys_for_customer(month, cust_id)
    if not keys:
        return pd.DataFrame()
    dfs = []
    for k in keys:
        try:
            dfs.append(load_parquet_from_s3(k))
        except Exception as e:
            print(f"Failed to load {k}: {e}")
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    # normalize column names
    df.columns = [c.lower() for c in df.columns]
    # ensure date col exists and parsed
    if COLS["date"] in df.columns:
        df[COLS["date"]] = pd.to_datetime(df[COLS["date"]])
    return df

def password_for_customer(row, cust_id):
    """Default password: last4(phone) else last4(cust_id)"""
    phone_col = COLS["phone"]
    phone = ""
    if phone_col in row and pd.notnull(row[phone_col]):
        phone = str(row[phone_col]).strip()
    if phone and len(phone) >= 4:
        return phone[-4:]
    return str(cust_id)[-4:]

def format_currency(x):
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return str(x)

# -------- PDF layout helpers --------
def _header_footer(canvas, doc, logo_img=None):
    canvas.saveState()
    # Header: logo left + bank name
    x_margin = 40
    y_top = PAGE_HEIGHT - 40
    if logo_img:
        try:
            canvas.drawImage(logo_img, x_margin, y_top - 40, width=80, height=30, mask='auto')
            canvas.setFont("Helvetica-Bold", 14)
            canvas.drawString(x_margin + 90, y_top - 10, "Your Bank Name")
        except Exception:
            canvas.setFont("Helvetica-Bold", 14)
            canvas.drawString(x_margin, y_top - 10, "Your Bank Name")
    else:
        canvas.setFont("Helvetica-Bold", 14)
        canvas.drawString(x_margin, y_top - 10, "Your Bank Name")

    # Footer
    canvas.setFont("Helvetica", 8)
    canvas.drawString(x_margin, 30, "This is a system generated statement. For queries contact support@yourdomain.com")
    canvas.restoreState()

def build_statement_pdf_bytes(cust_meta, df_txns, logo_local_path=None):
    """
    Returns bytes of the generated PDF (unencrypted).
    cust_meta: dict {cust_id, name, email, phone, acct, period}
    df_txns: DataFrame sorted by date for the period
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=PAGE_SIZE,
                            leftMargin=36, rightMargin=36,
                            topMargin=72, bottomMargin=72)

    # Page template to add header/footer
    def on_page(canvas, doc):
        _header_footer(canvas, doc, logo_local_path)

    story = []
    story.append(Paragraph("<b>Monthly Bank Statement</b>", styles["Title"]))
    story.append(Spacer(1, 6))
    # meta
    meta_html = (
        f"<b>Account Holder:</b> {cust_meta.get('name','')}<br/>"
        f"<b>Account No:</b> {cust_meta.get('acct','')}<br/>"
        f"<b>Statement Period:</b> {cust_meta.get('period','')}<br/>"
    )
    story.append(Paragraph(meta_html, styles["Normal"]))
    story.append(Spacer(1, 12))

    # summary calculations
    df_txns = df_txns.copy()
    if COLS["amt"] in df_txns.columns:
        df_txns[COLS["amt"]] = pd.to_numeric(df_txns[COLS["amt"]], errors="coerce").fillna(0)
    if COLS["bal"] in df_txns.columns:
        df_txns[COLS["bal"]] = pd.to_numeric(df_txns[COLS["bal"]], errors="coerce").fillna(0)

    total_credits = df_txns[df_txns[COLS["amt"]] >= 0][COLS["amt"]].sum() if COLS["amt"] in df_txns.columns else 0
    total_debits = df_txns[df_txns[COLS["amt"]] < 0][COLS["amt"]].sum() if COLS["amt"] in df_txns.columns else 0
    closing_balance = df_txns.iloc[-1][COLS["bal"]] if len(df_txns) and COLS["bal"] in df_txns.columns else 0

    summary_table = [
        ["Total Credits", format_currency(total_credits)],
        ["Total Debits", format_currency(abs(total_debits))],
        ["Closing Balance", format_currency(closing_balance)],
    ]
    t = Table(summary_table, hAlign='LEFT', colWidths=[2.5*inch, 2.0*inch])
    t.setStyle(TableStyle([('FONTSIZE', (0,0), (-1,-1), 9),
                            ('BOTTOMPADDING', (0,0), (-1,-1), 6)]))
    story.append(t)
    story.append(Spacer(1, 12))

    # Transaction Table header and rows
    cols_to_show = [COLS["date"], COLS["desc"], COLS["amt"], COLS["bal"]]
    header = ["Date", "Description", "Amount", "Balance"]
    table_data = [header]

    for _, r in df_txns.sort_values(COLS["date"]).iterrows():
        date = pd.to_datetime(r[COLS["date"]]).strftime("%Y-%m-%d") if pd.notnull(r[COLS["date"]]) else ""
        desc = str(r.get(COLS["desc"], ""))[:120]  # truncate long desc
        amt = format_currency(r.get(COLS["amt"], ""))
        bal = format_currency(r.get(COLS["bal"], ""))
        table_data.append([date, desc, amt, bal])

    # set col widths (adjust as needed)
    col_widths = [1.1*inch, 3.3*inch, 1.0*inch, 1.0*inch]
    tx_table = Table(table_data, repeatRows=1, colWidths=col_widths)
    tx_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#f1f1f1')),
        ('GRID', (0,0), (-1,-1), 0.25, colors.grey),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('ALIGN', (2,1), (3,-1), 'RIGHT'),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ]))

    story.append(tx_table)
    story.append(Spacer(1, 12))

    # small note
    story.append(Paragraph("Note: This statement is computer generated.", styles["Small"]))

    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
    buffer.seek(0)
    return buffer.read()

def encrypt_pdf_bytes(pdf_bytes, password):
    r = PdfReader(io.BytesIO(pdf_bytes))
    w = PdfWriter()
    for p in r.pages:
        w.add_page(p)
    w.encrypt(password)
    out_buf = io.BytesIO()
    w.write(out_buf)
    out_buf.seek(0)
    return out_buf.read()

def upload_bytes_to_s3(bytes_obj, s3_key, content_type="application/pdf"):
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=bytes_obj, ContentType=content_type)
    s3_url = f"s3://{S3_BUCKET}/{s3_key}"
    return s3_url

def send_pdf_via_ses(to_email, subject, body_text, pdf_bytes, filename="statement.pdf"):
    if not ses:
        print("SES not configured")
        return None
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = SENDER_EMAIL
    msg["To"] = to_email
    msg.attach(MIMEText(body_text, "plain"))
    part = MIMEApplication(pdf_bytes)
    part.add_header("Content-Disposition", "attachment", filename=filename)
    msg.attach(part)
    resp = ses.send_raw_email(RawMessage={"Data": msg.as_string()})
    return resp

# def send_sms_via_sns(phone_number, message):
#     if not sns:
#         print("SNS not configured")
#         return None
#     # phone_number must be in E.164 format like +919876543210
#     resp = sns.publish(PhoneNumber=phone_number, Message=message)
#     return resp

# -------- main pipeline --------
def process_month(month_str):
    """
    month_str: e.g. "2025-11"
    """
    # optional: download logo from S3 to local temp if LOGO_S3_KEY provided
    logo_local = None
    if LOGO_S3_KEY:
        try:
            logo_local = "/tmp/logo.png"
            s3.download_file(S3_BUCKET, LOGO_S3_KEY, logo_local)
        except Exception as e:
            print("Logo download failed:", e)
            logo_local = None

    customers = list_customer_folders(month_str)
    print("Found customers:", customers)

    for cust_id in customers:
        print(f"Processing {cust_id} ...")
        df = assemble_customer_df(month_str, cust_id)
        if df.empty:
            print(f"No transactions for {cust_id}, skipping.")
            continue

        # try extract meta from first row
        first = df.iloc[0] if len(df) else {}
        cust_meta = {
            "cust_id": cust_id,
            "name": first.get(COLS["name"], "") if hasattr(first, 'get') else "",
            "email": first.get(COLS["email"], "") if hasattr(first, 'get') else "",
            "phone": first.get(COLS["phone"], "") if hasattr(first, 'get') else "",
            "acct": first.get(COLS["acct"], "") if hasattr(first, 'get') else "",
            "period": month_str
        }

        # build pdf bytes
        try:
            pdf_bytes = build_statement_pdf_bytes(cust_meta, df, logo_local_path=logo_local)
        except Exception as e:
            print(f"Failed to build PDF for {cust_id}: {e}")
            continue

        # determine password
        pwd = password_for_customer(first if hasattr(first, 'get') else {}, cust_id)

        # encrypt
        enc_bytes = encrypt_pdf_bytes(pdf_bytes, pwd)

        # upload encrypted pdf to S3 (optional)
        s3_key = f"{OUTPUT_S3_PREFIX}/month={month_str}/{cust_id}_statement_{month_str}.pdf"
        if UPLOAD_TO_S3:
            try:
                upload_bytes_to_s3(enc_bytes, s3_key)
                print(f"Uploaded protected PDF to s3://{S3_BUCKET}/{s3_key}")
            except Exception as e:
                print("Upload failed:", e)

        # send via SES
        email_addr = cust_meta.get("email")
        if SEND_VIA_SES and email_addr and "@" in str(email_addr):
            subject = f"Your Monthly Bank Statement - {month_str}"
            body = (
                f"Dear {cust_meta.get('name','Customer')},\n\n"
                "Please find attached your password-protected monthly bank statement.\n"
                "For security, the password has been sent separately.\n\n"
                "Regards,\nYour Bank"
            )
            try:
                resp = send_pdf_via_ses(email_addr, subject, body, enc_bytes, filename=f"{cust_id}_statement_{month_str}.pdf")
                print("Email sent via SES:", resp)
            except Exception as e:
                print("SES send failed:", e)
        else:
            print(f"Skipping email for {cust_id} (no email or SES disabled).")

    print("Done processing month", month_str)

# -------- run example --------
if __name__ == "__main__":
    # Change month to the folder name you want to process, or compute last month automatically
    # Example: process_month("2025-11")
    # To auto pick last month:
    today = datetime.utcnow()
    first_of_this_month = datetime(today.year, today.month, 1)
    last_month_last_day = first_of_this_month - pd.Timedelta(days=1)
    month_to_process = f"{last_month_last_day.year}-{last_month_last_day.month:02d}"
    process_month(month_to_process)
