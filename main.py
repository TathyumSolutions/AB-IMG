"""
Email Agent + Extraction + Email Generation
- Monitors inbox, downloads attachments, runs field extraction
- After extraction_results.xlsx is created, generates and sends summary emails
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
import pandas as pd
from openpyxl import load_workbook
from docx import Document
from pypdf import PdfReader
import pdfplumber
from openai import OpenAI

# Import the CompleteEmailGenerator class from complete_email_generator.py
# Import required classes
from complete_email_generator import CompleteEmailGenerator
from email_agent_with_extraction import (
    EmailAgentWithExtraction,
    SUPPORTED_EXTENSIONS,
    read_document,
    extract_fields_with_intelligent_selection,
    OPENAI_API_KEY,
    OPENAI_MODEL,
    OUTPUT_FILENAME,
    merge_results_to_excel,
    CONFIG_FILE_PATH,
    load_config
)

# ...existing document reading, config, LLM extraction, and merging functions from email_agent_with_extraction.py...
# ...existing EmailAgentWithExtraction class from email_agent_with_extraction.py...

class EmailAgentWithExtractionAndEmail(EmailAgentWithExtraction):
    def run_once(self):
        """Execute single check cycle and exit"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("EMAIL AGENT - SINGLE EXECUTION MODE")
        self.logger.info("=" * 60)
        self.logger.info(f"Email: {self.email_address}")
        self.logger.info(f"Server: {self.imap_server}:{self.imap_port}")
        self.logger.info(f"Watching for subjects: {', '.join(self.target_subjects)}")
        self.logger.info(f"Loan ID Pattern: {self.loan_id_pattern}")
        self.logger.info(f"Root save location: {self.save_location}")
        self.logger.info("=" * 60 + "\n")
        
        if not self.connect():
            self.logger.error("[FAILED] Could not connect to email server")
            return
        
        try:
            self.logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Checking emails...")
            attachments = self.check_emails()
            
            if attachments:
                self.logger.info(f"[SUCCESS] Downloaded {len(attachments)} attachment(s) into {len(set(Path(f).parent for f in attachments))} folder(s)")
            else:
                self.logger.info("[INFO] No new attachments found")
            
            self.logger.info("[COMPLETE] Single check cycle finished")
        except Exception as e:
            self.logger.error(f"[ERROR] Execution failed: {e}")
        finally:
            self.disconnect()
            self.logger.info("[EXIT] Agent shutting down\n")

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
            # === EMAIL GENERATION HOOK ===
            self.run_email_generation(folder_path, output_path)
        else:
            self.logger.info(f"  [EXTRACTION] No extraction results to save for {folder_path}")

    def run_email_generation(self, folder_path, extraction_file):
        # Use corrected constructor and debug logic from CompleteEmailGenerator
        config_file = CONFIG_FILE_PATH
        recipients_config = os.path.join(folder_path, "abhl_imgc.json")
        smtp_config = "config.json"
        api_key = OPENAI_API_KEY
        generator = CompleteEmailGenerator(
            extraction_file=extraction_file,
            config_file=config_file,
            api_key=api_key,
            recipients_config=recipients_config,
            smtp_config=smtp_config
        )
        print(f"[DEBUG] Recipients loaded: {generator.recipients}")
        result = generator.generate_and_send_all_emails(folder_path, send_emails=True)
        print(f"[DEBUG] Email generation and sending complete for: {extraction_file}")
# ...main function and config loading as in email_agent_with_extraction.py...

def main():
    parser = argparse.ArgumentParser(description='Email Agent with Extraction and Email Generation')
    parser.add_argument('-c', '--config', default='config.json', help='Configuration file (default: config.json)')
    parser.add_argument('--once', action='store_true', help='Run once and exit (for cron scheduling)')
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
    agent = EmailAgentWithExtractionAndEmail(config)
    
    if args.once:
        agent.run_once()
    else:
        agent.run()

if __name__ == "__main__":
    main()
