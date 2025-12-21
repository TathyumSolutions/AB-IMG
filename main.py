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
    match_document_to_column,
    OPENAI_API_KEY,
    OPENAI_MODEL,
    OUTPUT_FILENAME,
    merge_results_to_excel,
    CONFIG_FILE_PATH,
    load_config,
    prepare_config_for_llm,
    get_description_columns
)

# class EmailAgentWithExtractionAndEmail(EmailAgentWithExtraction):
#     def __init__(self, config):
#         print("Initializing EmailAgentWithExtractionAndEmail...")
#         print("Loading field extraction config...", CONFIG_FILE_PATH)
#         super().__init__(config)
#         # Ensure config_structure is available for compatibility
#         self.config_structure = prepare_config_for_llm(self.config_df)
#         print(f"[INFO] Config structure prepared for LLM.")

#     def run_field_extraction(self, folder_path):
#         """Run field extraction on all documents in the folder using refactored logic"""
#         try:
#             self.logger.info(f"\n  [EXTRACTION] Starting field extraction for {folder_path}")
            
#             # Get all supported documents in the folder
#             documents = [
#                 f for f in os.listdir(folder_path)
#                 if Path(f).suffix.lower() in SUPPORTED_EXTENSIONS
#             ]
            
#             if not documents:
#                 self.logger.info(f"  [EXTRACTION] No supported documents found in {folder_path}")
#                 return
            
#             self.logger.info(f"  [EXTRACTION] Found {len(documents)} document(s) to process")
            
#             # ONE-TIME LLM CALL: Match all documents to columns
#             filename_to_column = match_document_to_column(
#                 documents=documents,
#                 description_columns=self.description_columns,
#                 folder_path=folder_path
#             )
            
#             self.logger.info(f"  [EXTRACTION] Document-to-column mapping completed")
            
#             # Extract fields from each document using its matched column
#             all_results = {}
#             column_selections = {}
            
#             for doc_file in documents:
#                 matched_column = filename_to_column.get(doc_file)
#                 if not matched_column:
#                     self.logger.warning(f"    [EXTRACTION] Document '{doc_file}' has no matching column. Skipping.")
#                     continue
                
#                 doc_path = os.path.join(folder_path, doc_file)
#                 doc_name = Path(doc_file).stem
                
#                 self.logger.info(f"    [EXTRACTION] Processing: {doc_file} (using column: {matched_column})")
                
#                 # Read document
#                 document_text = read_document(doc_path)
#                 if not document_text or len(document_text) < 50:
#                     self.logger.info(f"    [EXTRACTION] Skipping {doc_file} (not enough content)")
#                     continue
                
#                 # Extract fields using the matched column (pass matched_column directly)
#                 extracted_data, _ = extract_fields_with_intelligent_selection(
#                     document_text=document_text,
#                     config_df=self.config_df,
#                     matched_column=matched_column,
#                     pas_fields=self.pas_fields,
#                     api_key=OPENAI_API_KEY,
#                     model=OPENAI_MODEL
#                 )
                
#                 if extracted_data:
#                     all_results[doc_name] = extracted_data
#                     column_selections[doc_name] = matched_column
#                     self.logger.info(f"    [EXTRACTION] ✓ Extracted {len(extracted_data)} fields using column: {matched_column}")
            
#             # Save results
#             if all_results:
#                 output_path = os.path.join(folder_path, OUTPUT_FILENAME)
#                 merge_results_to_excel(all_results, self.pas_fields, output_path, column_selections)
#                 self.logger.info(f"  [EXTRACTION] Results saved to {output_path}")
                
#                 # === EMAIL GENERATION HOOK ===
#                 self.run_email_generation(folder_path, output_path)
#             else:
#                 self.logger.info(f"  [EXTRACTION] No extraction results to save for {folder_path}")
                
#         except Exception as e:
#             self.logger.error(f"  [EXTRACTION ERROR] {e}")
#             import traceback
#             self.logger.error(f"Traceback: {traceback.format_exc()}")

#     def run_email_generation(self, folder_path, extraction_file):
#         """Generate and send summary emails after extraction"""
#         try:
#             self.logger.info(f"\n  [EMAIL GENERATION] Starting email generation for {extraction_file}")
            
#             config_file = CONFIG_FILE_PATH
#             recipients_config = os.path.join(folder_path, "abhl_imgc.json")
#             smtp_config = "config.json"
#             api_key = OPENAI_API_KEY
            
#             # Check if recipients config exists
#             if not os.path.exists(recipients_config):
#                 self.logger.warning(f"  [EMAIL GENERATION] Recipients config not found: {recipients_config}")
#                 self.logger.info(f"  [EMAIL GENERATION] Skipping email generation")
#                 return
            
#             generator = CompleteEmailGenerator(
#                 extraction_file=extraction_file,
#                 config_file=config_file,
#                 api_key=api_key,
#                 recipients_config=recipients_config,
#                 smtp_config=smtp_config
#             )
            
#             self.logger.info(f"  [EMAIL GENERATION] Recipients loaded: {len(generator.recipients)} recipient(s)")
            
#             result = generator.generate_and_send_all_emails(folder_path, send_emails=True)
            
#             self.logger.info(f"  [EMAIL GENERATION] ✓ Email generation and sending complete")
            
#         except Exception as e:
#             self.logger.error(f"  [EMAIL GENERATION ERROR] {e}")
#             import traceback
#             self.logger.error(f"Traceback: {traceback.format_exc()}")

def main():
    print("Starting Email Agent with Extraction and Email Generation...")
    parser = argparse.ArgumentParser(description='Email Agent with Extraction and Email Generation')
    parser.add_argument('-c', '--config', default='config.json', help='Configuration file (default: config.json)')
    args = parser.parse_args()
    
    if not os.path.exists(args.config):
        print(f"[ERROR] Config file not found: {args.config}")
        return
    
    if not os.path.exists(CONFIG_FILE_PATH):
        print(f"[ERROR] Field extraction config not found: {CONFIG_FILE_PATH}")
        return
    
    # if not OPENAI_API_KEY:
    #     print(f"[ERROR] OPENAI_API_KEY not set in environment.")
    #     return
    print("Loading config...",args.config)
    config = load_config(args.config)
    print(f"[INFO] Loaded config from {args.config}")
    print("***EmailAddress:",config.get('email.address'))
    agent = EmailAgentWithExtraction(config)
    agent.run()
    print(f"[INFO] Email Agent with Extraction and Email Generation completed.")

if __name__ == "__main__":
    main()