#!/usr/bin/env python3
"""
Combined Email Agent and Document Field Extraction System
- Monitors email inbox, downloads attachments, and saves metadata
- After downloading all attachments for a new email, runs field extraction (LLM-based) for that folder only

Usage:
    export OPENAI_API_KEY='your-api-key-here'
    python email_agent_with_extraction.py
"""

import os
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime
import argparse
import re
import imaplib
import email
from email.header import decode_header
from dotenv import load_dotenv

# Document processing imports
import pandas as pd
from openpyxl import load_workbook
from docx import Document
from pypdf import PdfReader
import pdfplumber
from openai import OpenAI

load_dotenv()

# =============================================================================
# CONFIGURATION SECTION
# =============================================================================
CONFIG_FILE_PATH = "FieldConfigrationFile.xlsx"  # Configuration Excel file
OUTPUT_FILENAME = "extraction_results.xlsx"      # Output Excel filename (created in each folder)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = "gpt-4o"
MAX_DOCUMENT_CHARS = 15000
SUPPORTED_EXTENSIONS = ['.pdf', '.docx', '.doc', '.xlsx', '.xls']

# =============================================================================
# DOCUMENT READING FUNCTIONS (from extract_fields_intelligent.py)
# =============================================================================
def read_pdf_document(file_path: str) -> str:
    text_parts = []
    try:
        with pdfplumber.open(file_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(f"\n{'='*60}\n[PAGE {page_num}]\n{'='*60}\n")
                    text_parts.append(page_text)
                tables = page.extract_tables()
                if tables:
                    for table_num, table in enumerate(tables, 1):
                        text_parts.append(f"\n[TABLE {table_num} ON PAGE {page_num}]\n")
                        for row in table:
                            if row:
                                row_text = " | ".join(str(cell) if cell else "" for cell in row)
                                text_parts.append(row_text)
                        text_parts.append("")
        return "\n".join(text_parts)
    except Exception as e:
        print(f"  ⚠ Error reading PDF {file_path}: {e}")
        return ""

def read_word_document(file_path: str) -> str:
    text_parts = []
    try:
        doc = Document(file_path)
        text_parts.append("="*60)
        text_parts.append("[DOCUMENT CONTENT]")
        text_parts.append("="*60)
        for para in doc.paragraphs:
            if para.text.strip():
                text_parts.append(para.text)
        if doc.tables:
            text_parts.append("\n" + "="*60)
            text_parts.append("[TABLES]")
            text_parts.append("="*60)
            for table_num, table in enumerate(doc.tables, 1):
                text_parts.append(f"\n[TABLE {table_num}]")
                for row_num, row in enumerate(table.rows):
                    row_text = " | ".join(cell.text.strip() for cell in row.cells)
                    text_parts.append(row_text)
        return "\n".join(text_parts)
    except Exception as e:
        print(f"  ⚠ Error reading Word document {file_path}: {e}")
        return ""

def read_excel_document(file_path: str) -> str:
    text_parts = []
    try:
        wb = load_workbook(file_path, data_only=True)
        for sheet_name in wb.sheetnames:
            sheet = wb[sheet_name]
            text_parts.append("="*60)
            text_parts.append(f"[SHEET: {sheet_name}]")
            text_parts.append("="*60)
            for row in sheet.iter_rows(values_only=True):
                row_text = " | ".join(str(cell) if cell is not None else "" for cell in row)
                if row_text.strip(" |"):
                    text_parts.append(row_text)
        return "\n".join(text_parts)
    except Exception as e:
        print(f"  ⚠ Error reading Excel file {file_path}: {e}")
        return ""

def read_document(file_path: str) -> str:
    ext = Path(file_path).suffix.lower()
    if ext == '.pdf':
        return read_pdf_document(file_path)
    elif ext in ['.docx', '.doc']:
        return read_word_document(file_path)
    elif ext in ['.xlsx', '.xls']:
        return read_excel_document(file_path)
    else:
        print(f"  ⚠ Unsupported file format: {ext}")
        return ""

# =============================================================================
# CONFIGURATION MANAGEMENT (from extract_fields_intelligent.py)
# =============================================================================
def load_configuration(config_path: str):
    try:
        config_df = pd.read_excel(config_path, sheet_name='Sheet1')
        pas_fields = config_df['PAS Field Name'].tolist()
        return config_df, pas_fields
    except Exception as e:
        print(f"✗ Error loading configuration file: {e}")
        sys.exit(1)

def prepare_config_for_llm(config_df: pd.DataFrame) -> str:
    config_text = []
    description_columns = [col for col in config_df.columns if 'Description' in col]
    config_text.append("CONFIGURATION FILE STRUCTURE:")
    config_text.append("="*80)
    config_text.append(f"\nAvailable instruction columns: {len(description_columns)}")
    for i, col in enumerate(description_columns, 1):
        field_count = config_df[col].notna().sum()
        config_text.append(f"{i}. {col} ({field_count} fields with instructions)")
    config_text.append("\n" + "="*80)
    config_text.append("FIELD EXTRACTION INSTRUCTIONS:")
    config_text.append("="*80)
    for idx, row in config_df.iterrows():
        field_name = row['PAS Field Name']
        config_text.append(f"\n[FIELD: {field_name}]")
        for col in description_columns:
            description = row.get(col, '')
            if pd.notna(description) and str(description).strip():
                config_text.append(f"  • {col}: {str(description)}")
    return "\n".join(config_text)

# =============================================================================
# LLM-BASED EXTRACTION (from extract_fields_intelligent.py)
# =============================================================================
def extract_fields_with_intelligent_selection(document_text: str, config_structure: str, document_name: str, file_extension: str, pas_fields, api_key: str, model: str):
    if len(document_text) > MAX_DOCUMENT_CHARS:
        document_text = document_text[:MAX_DOCUMENT_CHARS]
    prompt = f"""You are an expert document field extraction system with intelligent configuration selection.\n\nTASK OVERVIEW:\nYou will receive a document and a complete configuration file with multiple instruction columns. Your job is to:\n1. Analyze the document name, type, and content to select the MOST APPROPRIATE instruction column\n2. Extract all fields using the instructions from that selected column\n3. Return the extracted data along with the column you selected\n\nDOCUMENT INFORMATION:\n- Document Name: {document_name}\n- File Extension: {file_extension}\n- Document Type: {{'Word Document' if file_extension in ['.docx', '.doc'] else 'PDF Document' if file_extension == '.pdf' else 'Excel Spreadsheet'}}\n\n{config_structure}\n\nDOCUMENT CONTENT:\n{document_text}\n\nEXTRACTION INSTRUCTIONS:\n1. FIRST: Analyze the document name \"{document_name}\" and type \"{file_extension}\"\n2. SELECT the most appropriate instruction column based on the rules above\n3. For each of the {len(pas_fields)} PAS fields, extract the value using instructions from your selected column\n4. If a field has no instruction in the selected column, mark as \"NO INSTRUCTION\"\n5. If a field has instruction but value is not found in document, mark as \"NOT FOUND\"\n6. Extract exact values as they appear in the document\n7. Do NOT make assumptions or infer values not explicitly stated\n\nOUTPUT FORMAT:\nReturn a JSON object with TWO keys:\n1. \"selected_column\": The name of the configuration column you selected\n2. \"extracted_fields\": An object with field names as keys and extracted values as values\n\nCRITICAL: Include ALL {len(pas_fields)} PAS fields in your response, even if they have \"NO INSTRUCTION\" or are \"NOT FOUND\".\nReturn ONLY the JSON object, no additional text."""
    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a precise document field extraction assistant with intelligent configuration selection. Always respond with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        result_text = response.choices[0].message.content
        result_json = json.loads(result_text)
        selected_column = result_json.get("selected_column", "UNKNOWN")
        extracted_fields = result_json.get("extracted_fields", {})
        for field in pas_fields:
            if field not in extracted_fields:
                extracted_fields[field] = "ERROR"
        return extracted_fields, selected_column
    except Exception as e:
        print(f"  ✗ LLM extraction error: {e}")
        return {field: "ERROR" for field in pas_fields}, "ERROR"

# =============================================================================
# RESULT MERGING AND OUTPUT (from extract_fields_intelligent.py)
# =============================================================================
def merge_results_to_excel(all_results, pas_fields, output_path, column_selections):
    results_data = []
    for field in pas_fields:
        row = {'PAS Field Name': field}
        for doc_name, extracted_data in all_results.items():
            row[doc_name] = extracted_data.get(field, "NOT PROCESSED")
        results_data.append(row)
    results_df = pd.DataFrame(results_data)
    metadata_data = []
    for doc_name, selected_col in column_selections.items():
        metadata_data.append({
            'Document Name': doc_name,
            'Selected Configuration Column': selected_col
        })
    metadata_df = pd.DataFrame(metadata_data)
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            results_df.to_excel(writer, sheet_name='Extracted Fields', index=False)
            metadata_df.to_excel(writer, sheet_name='Column Selections', index=False)
        print(f"\n✓ Results saved to: {output_path}")
    except Exception as e:
        print(f"\n✗ Error saving results: {e}")

# =============================================================================
# EMAIL AGENT (from EmailAgent.py, with hook for extraction)
# =============================================================================
class EmailAgentWithExtraction:
    def __init__(self, config):
        self.email_address = config['email']['address']
        self.password = config['email']['password']
        self.imap_server = config['email']['imap_server']
        self.imap_port = config['email'].get('imap_port', 993)
        self.target_subjects = config['agent']['target_subjects']
        if isinstance(self.target_subjects, str):
            self.target_subjects = [self.target_subjects]
        self.loan_id_pattern = config['agent']['loan_id_pattern']
        self.save_location = Path(config['agent']['save_location'])
        self.check_interval = config['agent'].get('check_interval', 60)
        self.mark_as_read = config['agent'].get('mark_as_read', False)
        self.only_unseen = config['agent'].get('only_unseen', True)
        self.processed_emails = set()
        self.mail = None
        self._setup_logging(config['agent'].get('log_file'))
        self.save_location.mkdir(parents=True, exist_ok=True)
        # Extraction config
        self.config_df, self.pas_fields = load_configuration(CONFIG_FILE_PATH)
        self.config_structure = prepare_config_for_llm(self.config_df)

    def _setup_logging(self, log_file=None):
        self.logger = logging.getLogger('EmailAgentWithExtraction')
        self.logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)
        if log_file:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_format)
            self.logger.addHandler(file_handler)

    def connect(self, retry_count=3, retry_delay=5):
        for attempt in range(retry_count):
            try:
                self.logger.info(f"Connecting to {self.imap_server}:{self.imap_port}...")
                self.mail = imaplib.IMAP4_SSL(self.imap_server, self.imap_port)
                self.mail.login(self.email_address, self.password)
                self.logger.info(f"[SUCCESS] Connected to {self.imap_server}")
                return True
            except imaplib.IMAP4.error as e:
                self.logger.error(f"[AUTH FAILED] {e}")
                return False
            except Exception as e:
                self.logger.warning(f"Connection attempt {attempt + 1}/{retry_count} failed: {e}")
                if attempt < retry_count - 1:
                    time.sleep(retry_delay)
        self.logger.error("[FAILED] Could not connect after all retries")
        return False

    def disconnect(self):
        if self.mail:
            try:
                self.mail.close()
                self.mail.logout()
                self.logger.info("[DISCONNECT] Logged out from server")
            except Exception as e:
                self.logger.warning(f"Error during disconnect: {e}")

    def decode_subject(self, subject):
        if not subject:
            return ""
        decoded_parts = decode_header(subject)
        decoded_subject = ""
        for part, encoding in decoded_parts:
            if isinstance(part, bytes):
                try:
                    decoded_subject += part.decode(encoding or 'utf-8', errors='replace')
                except Exception:
                    decoded_subject += part.decode('utf-8', errors='replace')
            else:
                decoded_subject += str(part)
        return decoded_subject

    def matches_subject(self, subject):
        subject_lower = subject.lower()
        return any(target.lower() in subject_lower for target in self.target_subjects)

    def extract_loan_id(self, subject):
        match = re.search(self.loan_id_pattern, subject)
        if match:
            loan_id = match.group(1).strip()
            invalid_chars = '<>:"|?*\\/\0'
            for char in invalid_chars:
                loan_id = loan_id.replace(char, '_')
            return loan_id
        return None

    def clean_filename(self, filename):
        invalid_chars = '<>:"|?*\\/\0'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        if len(filename) > 200:
            name, ext = os.path.splitext(filename)
            filename = name[:200 - len(ext)] + ext
        return filename

    def save_attachment(self, part, sub_folder):
        """
        Save email attachment to disk in the specified sub_folder
        Args:
            part: Email part containing attachment
            sub_folder (Path): Path to the unique folder for this email
        Returns:
            str: Filepath if saved successfully, None otherwise
        """
        filename = part.get_filename()
        if not filename:
            return None
        try:
            # Decode filename if needed
            if isinstance(filename, str):
                filename = self.clean_filename(filename)
            else:
                decoded = decode_header(filename)[0]
                if isinstance(decoded[0], bytes):
                    filename = decoded[0].decode(decoded[1] or 'utf-8')
                else:
                    filename = decoded[0]
                filename = self.clean_filename(filename)
            # Do NOT add timestamp to filename, just use original name
            unique_filename = filename
            filepath = sub_folder / unique_filename
            # Save the file
            with open(filepath, 'wb') as f:
                f.write(part.get_payload(decode=True))
            file_size = filepath.stat().st_size / 1024  # KB
            self.logger.info(f"  [SAVED ATTACHMENT] {unique_filename} ({file_size:.1f} KB)")
            return str(filepath)
        except Exception as e:
            self.logger.error(f"  [ERROR] Failed to save attachment: {e}")
            return None

    def _save_metadata(self, sub_folder, subject, mail_body, from_addr, date, loan_id, attachments):
        try:
            subject_path = sub_folder / "mail_subject.txt"
            with open(subject_path, 'w', encoding='utf-8') as f:
                f.write(subject)
            body_path = sub_folder / "mail_body.txt"
            with open(body_path, 'w', encoding='utf-8') as f:
                f.write(mail_body)
            log_path = sub_folder / "log.json"
            log_data = {
                "sender": from_addr,
                "timestamp": date,
                "loan_id": loan_id,
                "attachments": [Path(a).name for a in attachments],
                "subject": subject,
                "mail_body_initial": mail_body[:200]
            }
            with open(log_path, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, indent=2, ensure_ascii=False)
            abhl_imgc_path = sub_folder / "abhl_imgc.json"
            abhl_imgc_data = {
                "ABHL": from_addr,
                "IMGC": self.email_address
            }
            with open(abhl_imgc_path, 'w', encoding='utf-8') as f:
                json.dump(abhl_imgc_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.logger.error(f"  [ERROR] Failed to save metadata: {e}")

    def get_mail_body(self, msg):
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = part.get_content_disposition()
                if content_type == "text/plain" and content_disposition != "attachment":
                    try:
                        body += part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='replace')
                    except Exception:
                        body += part.get_payload(decode=True).decode('utf-8', errors='replace')
        else:
            if msg.get_content_type() == "text/plain":
                try:
                    body += msg.get_payload(decode=True).decode(msg.get_content_charset() or 'utf-8', errors='replace')
                except Exception:
                    body += msg.get_payload(decode=True).decode('utf-8', errors='replace')
        return body.strip()

    def process_email(self, email_id, msg):
        attachments_saved = []
        try:
            subject = self.decode_subject(msg.get('subject', ''))
            if not self.matches_subject(subject):
                return attachments_saved
            loan_id = self.extract_loan_id(subject)
            if not loan_id:
                self.logger.warning(f"\n[MATCH FOUND] Subject matches, but **could not extract Loan ID** for folder creation. Skipping...")
                self.logger.warning(f"  Subject: {subject}")
                return attachments_saved
            from_addr = msg.get('from', 'Unknown')
            date = msg.get('date', 'Unknown')
            loan_folder = self.save_location / loan_id
            loan_folder.mkdir(parents=True, exist_ok=True)
            folder_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            sub_folder = loan_folder / folder_timestamp
            sub_folder.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"\n[MATCH FOUND] Subject: {subject}")
            self.logger.info(f"  Loan ID: **{loan_id}**")
            self.logger.info(f"  Folder: {loan_id}/{folder_timestamp}")
            self.logger.info(f"  From: {from_addr}")
            self.logger.info(f"  Date: {date}")
            mail_body = self.get_mail_body(msg)
            attachment_count = 0
            if msg.is_multipart():
                for part in msg.walk():
                    content_disposition = part.get_content_disposition()
                    if content_disposition == 'attachment' and part.get_filename():
                        attachment_count += 1
                        filepath = self.save_attachment(part, sub_folder)
                        if filepath:
                            attachments_saved.append(filepath)
            else:
                if msg.get_content_disposition() == 'attachment' and msg.get_filename():
                    attachment_count += 1
                    filepath = self.save_attachment(msg, sub_folder)
                    if filepath:
                        attachments_saved.append(filepath)
            if attachment_count == 0:
                self.logger.info("  (No attachments found in email)")
            self._save_metadata(sub_folder, subject, mail_body, from_addr, date, loan_id, attachments_saved)
            # === FIELD EXTRACTION HOOK ===
            if attachments_saved:
                self.logger.info(f"  [EXTRACTION] Running field extraction for folder: {sub_folder}")
                self.run_field_extraction(sub_folder)
            if self.mark_as_read and attachments_saved:
                try:
                    self.mail.store(email_id, '+FLAGS', '\\Seen')
                except Exception as e:
                    self.logger.warning(f"  Could not mark email as read: {e}")
        except Exception as e:
            self.logger.error(f"[ERROR] Processing email: {e}")
        return attachments_saved

    def run_field_extraction(self, folder_path):
        # Only process this folder, not recursively
        documents = [f for f in os.listdir(folder_path) if Path(f).suffix.lower() in SUPPORTED_EXTENSIONS]
        if not documents:
            self.logger.info(f"  [EXTRACTION] No supported documents found in {folder_path}")
            return
        all_results = {}
        column_selections = {}
        for doc_file in documents:
            doc_path = os.path.join(folder_path, doc_file)
            doc_name = Path(doc_file).stem
            doc_ext = Path(doc_file).suffix
            document_text = read_document(doc_path)
            if not document_text or len(document_text) < 50:
                self.logger.info(f"    [EXTRACTION] Skipping {doc_file} (not enough content)")
                continue
            extracted_data, selected_column = extract_fields_with_intelligent_selection(
                document_text=document_text,
                config_structure=self.config_structure,
                document_name=doc_name,
                file_extension=doc_ext,
                pas_fields=self.pas_fields,
                api_key=OPENAI_API_KEY,
                model=OPENAI_MODEL
            )
            if extracted_data:
                all_results[doc_name] = extracted_data
                column_selections[doc_name] = selected_column
        if all_results:
            output_path = os.path.join(folder_path, OUTPUT_FILENAME)
            merge_results_to_excel(all_results, self.pas_fields, output_path, column_selections)
            self.logger.info(f"  [EXTRACTION] Results saved to {output_path}")
        else:
            self.logger.info(f"  [EXTRACTION] No extraction results to save for {folder_path}")

    def check_emails(self):
        all_attachments = []
        try:
            try:
                self.mail.select('inbox')
            except Exception:
                self.logger.warning("Connection lost, reconnecting...")
                if not self.connect():
                    return all_attachments
                self.mail.select('inbox')
            search_criteria = 'UNSEEN' if self.only_unseen else 'ALL'
            status, messages = self.mail.search(None, search_criteria)
            if status != 'OK':
                self.logger.error(f"Search failed: {status}")
                return all_attachments
            email_ids = messages[0].split()
            for email_id in email_ids:
                if email_id in self.processed_emails:
                    continue
                try:
                    status, msg_data = self.mail.fetch(email_id, '(RFC822)')
                    if status != 'OK':
                        continue
                    for response_part in msg_data:
                        if isinstance(response_part, tuple):
                            msg = email.message_from_bytes(response_part[1])
                            attachments = self.process_email(email_id, msg)
                            all_attachments.extend(attachments)
                    self.processed_emails.add(email_id)
                except Exception as e:
                    self.logger.error(f"[ERROR] Fetching email {email_id.decode('utf-8')}: {e}")
                    continue
        except Exception as e:
            self.logger.error(f"[ERROR] Checking emails: {e}")
        return all_attachments

    def run(self):
        self.logger.info("\n" + "=" * 60)
        self.logger.info("EMAIL AGENT WITH FIELD EXTRACTION STARTED")
        self.logger.info("=" * 60)
        self.logger.info(f"Email: {self.email_address}")
        self.logger.info(f"Server: {self.imap_server}:{self.imap_port}")
        self.logger.info(f"Watching for subjects: {', '.join(self.target_subjects)}")
        self.logger.info(f"Loan ID Pattern: {self.loan_id_pattern}")
        self.logger.info(f"Root save location: {self.save_location}")
        self.logger.info(f"Check interval: {self.check_interval}s")
        self.logger.info(f"Only unseen emails: {self.only_unseen}")
        self.logger.info(f"Mark as read: {self.mark_as_read}")
        self.logger.info("=" * 60 + "\n")
        if not self.connect():
            self.logger.error("[FAILED] Could not start agent - connection failed")
            return
        try:
            while True:
                self.logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Checking emails...")
                attachments = self.check_emails()
                if attachments:
                    self.logger.info(f"[SUCCESS] Downloaded {len(attachments)} attachment(s) into {len(set(Path(f).parent for f in attachments))} folder(s)")
                else:
                    self.logger.info("  No new attachments")
                time.sleep(self.check_interval)
        except KeyboardInterrupt:
            self.logger.info("\n\n[STOPPED] Agent stopped by user")
        except Exception as e:
            self.logger.error(f"\n\n[CRASH] Agent crashed: {e}")
        finally:
            self.disconnect()

# =============================================================================
# CONFIG LOADING AND MAIN
# =============================================================================
def load_config(config_file):
    with open(config_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def main():
    parser = argparse.ArgumentParser(description='Email Agent with Field Extraction')
    parser.add_argument('-c', '--config', default='config.json', help='Configuration file (default: config.json)')
    args = parser.parse_args()
    if not os.path.exists(args.config):
        print(f"[ERROR] Config file not found: {args.config}")
        return
    if not os.path.exists(CONFIG_FILE_PATH):
        print(f"[ERROR] Field extraction config not found: {CONFIG_FILE_PATH}")
        return
    if not OPENAI_API_KEY:
        print(f"[ERROR] OPENAI_API_KEY not set in environment.")
        return
    config = load_config(args.config)
    agent = EmailAgentWithExtraction(config)
    agent.run()

if __name__ == "__main__":
    main()
