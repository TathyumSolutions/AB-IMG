"""
Complete Email Generator - All Features in One File
====================================================
Features:
- Data cleaning (whitespace, special chars, case-insensitive)
- First/Second preference support
- Major field error messages
- Email sending via SMTP
- GPT integration (optional)
- ABHL email now includes Excel attachment with high criticality issues
"""

import pandas as pd
import os
import json
import smtplib
import shutil
from openai import OpenAI,AzureOpenAI
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
# from email_agent_with_extraction import llm_logger
import glob


def _ensure_txt_copy_for_attachment(source_path: str) -> str:
    base, _ = os.path.splitext(source_path)
    txt_path = base + ".txt"
    try:
        if (not os.path.exists(txt_path)) or (os.path.getmtime(txt_path) < os.path.getmtime(source_path)):
            shutil.copyfile(source_path, txt_path)
    except Exception:
        return source_path
    return txt_path

class CompleteEmailGenerator:
    """Complete email generator with all features and detailed logging"""
    def __init__(self, extraction_file, config_file, api_key, smtp_config):
        print(f"[LOG] Initializing CompleteEmailGenerator")
        self.extraction_file = extraction_file
        self.config_file = config_file
        self.api_key = api_key
        self.smtp_config = self.load_smtp_config(smtp_config)
        print(f"[LOG] Loading recipients from {smtp_config}")
        self.recipients = self.load_recipients_from_config(smtp_config)
        print(f"[LOG] Recipients loaded: {self.recipients}")
        print(f"[LOG] Loading extraction results from {self.extraction_file}")
        self.merged_df = self.load_extraction_results()
        print(f"[LOG] Extraction results loaded: {self.merged_df.shape[0]} rows, {self.merged_df.shape[1]} columns")
        self.client = None
        if self.api_key:
            try:
                print(f"[LOG] Initializing OpenAI client")
                self.client = OpenAI(api_key=self.api_key)
                print(f"[LOG] OpenAI client initialized")
            except Exception as e:
                print(f"[ERROR] Failed to initialize OpenAI client: {e}")


    def load_smtp_config(self, smtp_config_path):
        try:
            with open(smtp_config_path, "r") as f:
                config = json.load(f)
                print(f"[LOG] SMTP config loaded: {config['email']['address']}")
                return config['email']
        except Exception as e:
            print(f"[ERROR] Could not load SMTP config: {e}")
            return {}

    def load_recipients_from_config(self, config_path):
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
                abhl = config.get("abhl_imgc", {}).get("abhl_email_id", "")
                imgc = config.get("abhl_imgc", {}).get("imgc_email_id", "")
                return {"ABHL": abhl, "IMGC": imgc}
        except Exception as e:
            print(f"[ERROR] Could not load recipients from config: {e}")
            return {"ABHL": "", "IMGC": ""}

    def load_extraction_results(self):
        try:
            df = pd.read_excel(self.extraction_file)
            print(f"[LOG] Extraction results DataFrame loaded with shape {df.shape}")
            return df
        except Exception as e:
            print(f"[ERROR] Could not load extraction results: {e}")
            return pd.DataFrame()

    def _clean_value(self, val):
        if val is None:
            #print(f"[LOG] Cleaning value: None -> ''")
            return ""
        if isinstance(val, float) and pd.isna(val):
            #print(f"[LOG] Cleaning value: NaN -> ''")
            return ""
        cleaned = str(val).strip().lower()
        #print(f"[LOG] Cleaning value: {val} -> {cleaned}")
        return cleaned

    def _get_preferred_value(self, row, doc_columns):
        print(f"[LOG] Getting preferred value for row: {row.get('PAS Field Name', '')}")
        second_pref = row.get('Second Preference')
        first_pref = row.get('First Preference')
        if pd.notna(first_pref) and first_pref in doc_columns:
            val = self._clean_value(row[first_pref])
            if val:
                print(f"[LOG] First preference found: {first_pref} -> {val}")
                return val, first_pref
        if pd.notna(second_pref) and second_pref in doc_columns:
            val = self._clean_value(row[second_pref])
            if val:
                print(f"[LOG] Second preference found: {second_pref} -> {val}")
                return val, second_pref
        for col in doc_columns:
            val = self._clean_value(row[col])
            if val:
                print(f"[LOG] Value found in column {col}: {val}")
                return val, col
        print(f"[LOG] No preferred value found")
        return None, None

    def identify_issues(self):
        print(f"[LOG] Identifying issues in extraction results")
        issues = []
        exclude_cols = ['PAS Field Name', 'Mismatch Criticality', 'Criticality', 'First Preference', 'Second Preference', 'Final Data for PAS System']
        doc_columns = [col for col in self.merged_df.columns if col not in exclude_cols]
        for idx, row in self.merged_df.iterrows():
            field_name = row['PAS Field Name']
            criticality = row.get('Mismatch Criticality', row.get('Criticality', 'Unknown'))
            cleaned_values = {}
            raw_values = {}
            for col in doc_columns:
                raw_val = row[col]
                cleaned_val = self._clean_value(raw_val)
                if cleaned_val:
                    cleaned_values[col] = cleaned_val
                    raw_values[col] = raw_val
            unique_values = list(set(cleaned_values.values()))
            unique_count = len(unique_values)
            preferred_value, preferred_source = self._get_preferred_value(row, doc_columns)
            if unique_count != 1:
                print(f"[LOG] Issue found for field {field_name}: unique_count={unique_count}, values={unique_values}")
                if str(criticality).upper() == 'MAJOR':
                    error_type = "CRITICAL ERROR"
                    error_msg = f"❌ MAJOR FIELD ERROR: Multiple different values found or no valid value"
                else:
                    error_type = "Warning"
                    error_msg = f"⚠️  Inconsistent values found"
                issue_detail = {
                    'Field Name': field_name,
                    'Criticality': criticality,
                    'Error Type': error_type,
                    'Unique Values Count': unique_count,
                    'Values Found': unique_values if unique_count > 0 else ['No valid values found'],
                    'Preferred Value': preferred_value if preferred_value else 'None',
                    'Preferred Source': preferred_source if preferred_source else 'None',
                    'Document Sources': raw_values,
                    'Error Message': error_msg
                }
                issues.append(issue_detail)
        print(f"[LOG] Total issues identified: {len(issues)}")
        return pd.DataFrame(issues)

    def get_major_issues(self):
        print(f"[LOG] Getting major (high criticality) issues")
        all_issues = self.identify_issues()
        if all_issues.empty:
            print(f"[LOG] No issues found")
            return all_issues
        major_issues = all_issues[all_issues['Criticality'].astype(str).str.upper() == 'HIGH']
        print(f"[LOG] Major issues found: {len(major_issues)}")
        return major_issues

    def get_low_issues(self):
        print(f"[LOG] Getting low criticality issues")
        all_issues = self.identify_issues()
        if all_issues.empty:
            print(f"[LOG] No issues found")
            return all_issues
        low_issues = all_issues[all_issues['Criticality'].astype(str).str.upper() != 'HIGH']
        print(f"[LOG] Low issues found: {len(low_issues)}")
        return low_issues

    def format_issues_for_email(self, issues_df):
        print(f"[LOG] Formatting issues for email")
        if issues_df.empty:
            return "✅ No issues found. All data is consistent."
        formatted_text = ""
        for idx, row in issues_df.iterrows():
            field = row['Field Name']
            criticality = row['Criticality']
            error_type = row['Error Type']
            values = row['Values Found']
            preferred_val = row['Preferred Value']
            preferred_src = row['Preferred Source']
            formatted_text += f"\n{'='*70}\n"
            formatted_text += f"Field: {field}\n"
            formatted_text += f"Criticality: {criticality}\n"
            formatted_text += f"Issue Type: {error_type}\n"
            formatted_text += f"Values Found: {values}\n"
            if preferred_val != 'None':
                formatted_text += f"Preferred Value: {preferred_val} (from {preferred_src})\n"
            else:
                formatted_text += f"Preferred Value: None - Field missing or empty in all documents\n"
            if isinstance(row['Document Sources'], dict):
                formatted_text += "Document Sources:\n"
                for doc, val in row['Document Sources'].items():
                    formatted_text += f"  • {doc}: {val}\n"
            formatted_text += f"{'='*70}\n"
        return formatted_text

    def send_email(self, to_email, subject, body, attachment_path=None, attachment_paths=None):
        print(f"[LOG] Attempting to send email to: {to_email}")
        if not self.smtp_config:
            print("[ERROR] SMTP config not loaded. Cannot send email.")
            return False
        try:
            msg = MIMEMultipart()
            msg['From'] = self.smtp_config['address']
            if isinstance(to_email, (list, tuple)):
                msg['To'] = ', '.join(to_email)
                recipients = list(to_email)
            else:
                msg['To'] = to_email
                recipients = [to_email]
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            files_to_attach = []
            if attachment_paths is not None:
                if isinstance(attachment_paths, (list, tuple)):
                    files_to_attach.extend(list(attachment_paths))
                else:
                    files_to_attach.append(attachment_paths)
            if attachment_path is not None:
                files_to_attach.append(attachment_path)

            for path in files_to_attach:
                if not path:
                    continue
                if not os.path.exists(path):
                    print(f"[WARNING] Attachment path provided but file does not exist: {path}")
                    continue
                print(f"[LOG] Attaching file: {path}")
                with open(path, 'rb') as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                encoders.encode_base64(part)
                filename = os.path.basename(path)
                part.add_header('Content-Disposition', f'attachment; filename= {filename}')
                msg.attach(part)
            else:
                if attachment_path:
                    print(f"[WARNING] Attachment path provided but file does not exist: {attachment_path}")
            print(f"[LOG] Connecting to SMTP server: {self.smtp_config['smtp_server']}:{self.smtp_config['smtp_port']}")
            server = smtplib.SMTP(self.smtp_config['smtp_server'], self.smtp_config['smtp_port'])
            server.starttls()
            print(f"[LOG] Logging in with email: {self.smtp_config['address']}")
            server.login(self.smtp_config['address'], self.smtp_config['password'])
            text = msg.as_string()
            server.sendmail(self.smtp_config['address'], recipients, text)
            server.quit()
            print(f"[LOG] ✅ Email sent successfully to {recipients}")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to send email: {e}")
            return False

    def generate_email_with_gpt(self, recipient, subject_hint, body_content, context):
        print(f"[LOG] Generating email with GPT for recipient: {recipient}")
        if not self.client:
            print("[WARNING] GPT client not available. Using default email format.")
            return {
                'subject': subject_hint,
                'body': body_content
            }
        try:
            prompt = f"""You are a professional business email writer. Generate a professional email with the following specifications:

Recipient: {recipient}
Subject Line Hint: {subject_hint}
Context: {context}

Body Content:
{body_content}

Please create:
1. A professional subject line (concise but informative)
2. A polished email body that includes the provided content but with professional tone and structure

Return ONLY in this exact JSON format:
{{
    "subject": "your subject line here",
    "body": "your email body here"
}}
"""
            print(f"[LOG] Calling GPT API...")
            client = AzureOpenAI(
            azure_endpoint="https://qc-tspl-dau-mr.openai.azure.com/",
            api_key="DvskuzopcDYytzJygTQiCl1ikUiT8513H8vfpIwVPZPOnfeHCdZ1JQQJ99BEACHYHv6XJ3w3AAABACOGprIt",
            api_version="2025-01-01-preview",
        )

            completion = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                    "role": "system", 
                    "content": "You are a professional business email writer. Always return valid JSON"
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=16384,
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            result = completion.choices[0].message.content.strip()
            llm_logger.info(json.dumps({
                "model": "gpt-4o-mini",
                "input_tokens": completion.usage.prompt_tokens,
                "output_tokens": completion.usage.completion_tokens,
                "prompt": prompt,
                "response": result
            }))
            if result.startswith("```json"):
                result = result[7:]
            if result.endswith("```"):
                result = result[:-3]
            result = result.strip()
            email_data = json.loads(result)
            print(f"[LOG] Email generated successfully via GPT")
            return email_data
        except Exception as e:
            print(f"[ERROR] GPT email generation failed: {e}. Using default format.")
            return {
                'subject': subject_hint,
                'body': body_content
            }

    # def create_high_criticality_excel(self, output_folder):
    #     """Create an Excel file containing only high criticality rows, with filtered columns."""
    #     print(f"[LOG] Creating Excel file with high criticality rows")
    #     crit_col = 'Mismatch Criticality' if 'Mismatch Criticality' in self.merged_df.columns else 'Criticality'
    #     high_crit_df = self.merged_df[self.merged_df[crit_col].astype(str).str.upper() == 'HIGH'].copy()

    #     if high_crit_df.empty:
    #         print("[LOG] No high criticality rows found.")
    #         return None

    #     # Load config to get all columns
    #     config_df = pd.read_excel(self.config_file)
    #     all_columns = config_df.columns.tolist()

    #     # Columns to exclude
    #     exclude_keywords = ['Data Type', 'Field length', 'Primary Source Document', 'Secondary Source Document']
    #     exclude_columns = [col for col in all_columns if any(key in col for key in exclude_keywords) or 'Description' in col]

    #     # Columns to keep
    #     keep_columns = [col for col in all_columns if col not in exclude_columns]

    #     # Reorder and filter columns
    #     filtered_df = high_crit_df.reindex(columns=keep_columns)

    #     # Create filename with timestamp
    #     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #     filename = os.path.join(output_folder, f"High_Criticality_Issues_{timestamp}.xlsx")

    #     # Save to Excel
    #     filtered_df.to_excel(filename, index=False)
    #     print(f"[LOG] High criticality Excel file created: {filename}")
    #     print(f"[LOG] Rows in high criticality file: {len(filtered_df)}")

    #     return filename

    def create_issues_excel(self, output_folder):
        """Create an Excel file with all rows, filtered columns (for both ABHL and IMGC)."""
        print(f"[LOG] Creating issues Excel file for both ABHL and IMGC")
        # Load config to get all columns
        config_df = pd.read_excel(self.config_file)
        all_columns = config_df.columns.tolist()

        # Columns to exclude
        exclude_keywords = ['Data Type', 'Field length', 'Primary Source Document', 'Secondary Source Document']
        exclude_columns = [col for col in all_columns if any(key in col for key in exclude_keywords) or 'Description' in col]

        # Columns to keep
        keep_columns = [col for col in self.merged_df.columns if col not in exclude_columns]

        # Filter DataFrame
        filtered_df = self.merged_df[keep_columns]

        # Create filename
        filename = os.path.join(output_folder, "issues.xlsx")
        filtered_df.to_excel(filename, index=False)
        print(f"[LOG] Issues Excel file created: {filename}")
        return filename

    def _get_document_names_from_mapping(self, mapping_json_path):
        with open(mapping_json_path, 'r') as f:
            mapping = json.load(f)
        return list(mapping.keys())

    def generate_abhl_email(self, mapping_json_path):
        print(f"[LOG] Generating ABHL email (high criticality issues)")
        print("[LOG] Loading major issues for ABHL email mapping JSON ", mapping_json_path  )
        major_issues = self.get_major_issues()
        loan_id = self._extract_loan_id()

        doc_names = self._get_document_names_from_mapping(mapping_json_path)
        num_docs = len(doc_names)
        doc_list = '\n'.join([f"• {doc}" for doc in doc_names])

        if major_issues.empty:
            body_content = f"""Dear ABHFL Team,
I hope this email finds you well.
We are pleased to inform you that the documents shared to initiate the loan application have been successfully processed through our data extraction and quality check workflow.

Quality Check Summary:
• No discrepancies were identified
• Data values are consistent across the submitted documents

At this stage, no additional information or revised documents are required. However, we will keep you informed for any further inputs be needed during subsequent processing.

Documents Processed
A total of {num_docs} documents were received and processed, including:
{doc_list}

If you require any additional information or clarification, please feel free to reach out to us.
Warm regards,
IMGC Team
________________________________________
This is a system-generated email. Please do not reply to this message.
________________________________________
For Implementation Use Only
"""
            subject_hint = f"Loan ID: {loan_id} Loan Application Document Processing Update"
        else:
            # Summarization report for major mismatches
            summary_lines = [
                f"Dear ABHFL Team,",
                "I hope this email finds you well.",
                "We have processed your documents, but major discrepancies were identified during our quality check.",
                "",
                "Quality Check Summary:",
                f"• {len(major_issues)} major mismatches detected",
                "• Revised documents or additional information may be required.",
                "",
                "Documents Processed",
                f"A total of {num_docs} documents were received and processed, including:",
                f"{doc_list}",
                "",
                "Please review the attached summarization report for details on the discrepancies.",
                "If you require any additional information or clarification, please feel free to reach out to us.",
                "Warm regards,",
                "IMGC Team",
                "________________________________________",
                "This is a system-generated email. Please do not reply to this message.",
                "________________________________________",
                "For Implementation Use Only"
            ]
            body_content = '\n'.join(summary_lines)
            subject_hint = f"Loan ID: {loan_id} Loan Application Document Processing Update - Major Discrepancies Found"

        email = {
            'subject': subject_hint,
            'body': body_content
        }
        print(f"[LOG] ABHL email generated")
        return email

    def generate_imgc_email(self, mapping_json_path):
        print(f"[LOG] Generating IMGC email (criticality analysis)")
        loan_id = self._extract_loan_id()
        print("IMGC Loan Id:", loan_id)
        doc_names = self._get_document_names_from_mapping(mapping_json_path)
        num_docs = len(doc_names)
        print("IMGC JSON path:",mapping_json_path)
        print("IMGC doc_names:",doc_names)
        doc_list = '\n'.join([f"• {doc}" for doc in doc_names])

        # Extraction statistics
        total_fields = len(self.merged_df)
        all_issues = self.identify_issues()
        total_issues = len(all_issues)
        high_issues = all_issues[all_issues['Criticality'].astype(str).str.upper() == 'HIGH']
        low_issues = all_issues[all_issues['Criticality'].astype(str).str.upper() != 'HIGH']
        high_count = len(high_issues)
        low_count = len(low_issues)

        subject = f"ABHFL – Loan ID: {loan_id} – Document Data Extraction Report with Criticality Analysis"
        body = f"""Dear IMGC Team,

Subject - ABHFL – Loan ID: {loan_id} – Document Data Extraction Report with Criticality Analysis
I hope you are doing well.
A total of {num_docs} loan-related documents were received and successfully processed as part of this request. The documents include:
{doc_list}
Please find below a summary of the data extraction performed on the received documents, including overall extraction statistics and the identified high- and low-criticality issues:

📊 Data Extraction Summary
• Total Fields Processed: {total_fields}
• Total Issues Identified: {total_issues}
  o High Criticality Issues: {high_count}
  o Low Criticality Issues: {low_count}

The complete extracted Excel file has been attached for review and audit purposes.
If any clarification, correction, or follow-up action is required, please coordinate internally as per the defined workflow.
________________________________________
This is a system-generated email. Please do not reply to this message.
"""

        email = {
            'subject': subject,
            'body': body
        }
        print(f"[LOG] IMGC email generated")
        return email

    def save_email_to_file(self, email_dict, filename):
        print(f"[LOG] Saving email to file: {filename}")
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"Subject: {email_dict['subject']}\n")
            f.write("="*80 + "\n\n")
            f.write(email_dict['body'])
        print(f"[LOG] Email saved to: {filename}")
    
    def generate_and_send_all_emails(self, output_dir, send_emails=True):
        print("\n" + "="*80)
        print("EMAIL GENERATION WITH DATA CLEANING & PREFERENCES")
        print("="*80 + "\n")
        os.makedirs(output_dir, exist_ok=True)
        print(f"[LOG] Output directory ensured: {output_dir}")
        
        # Generate ABHL email
        print("📧 Generating ABHL email (High Criticality Issues)...")
        mapping_json=os.path.join(output_dir, f"document_column_mapping.json")
        abhl_email = self.generate_abhl_email(mapping_json)
        abhl_file = f"{output_dir}/email_to_ABHL.txt"
        self.save_email_to_file(abhl_email, abhl_file)
        
        # # Create high criticality Excel attachment for ABHL
        # print("📊 Creating high criticality Excel file for ABHL...")
        # abhl_attachment = self.create_high_criticality_excel(output_dir)
        
        # Create issues Excel attachment for both ABHL and IMGC
        print("📊 Creating issues Excel file for both ABHL and IMGC...")
        issues_attachment = self.create_issues_excel(output_dir)

        # Send ABHL email with attachment
        abhl_sent = False
        if send_emails and self.recipients.get('ABHL'):
            print(f"📤 Sending email to ABHL ({self.recipients['ABHL']})...")
            abhl_sent = self.send_email(
                to_email=self.recipients['ABHL'],
                subject=abhl_email['subject'],
                body=abhl_email['body'],
                attachment_path=issues_attachment
            )

        # # Send IMGC email with the same attachment
        # imgc_sent = False
        # if send_emails and self.recipients.get('IMGC'):
        #     print(f"\n📤 Sending email to IMGC ({self.recipients['IMGC']})...")
        #     imgc_sent = self.send_email(
        #         to_email=self.recipients['IMGC'],
        #         subject=imgc_email['subject'],
        #         body=imgc_email['body'],
        #         attachment_path=issues_attachment
        #     )
        
        # Generate IMGC email
        print("\n📧 Generating IMGC email (Low Criticality Issues)...")
        mapping_json=os.path.join(output_dir, f"document_column_mapping.json")
        imgc_email = self.generate_imgc_email(mapping_json)
        imgc_file = f"{output_dir}/email_to_IMGC.txt"
        self.save_email_to_file(imgc_email, imgc_file)
        
        print("[DEBUG] IMGC email body to be sent:\n", imgc_email['body'])
        
        # IMGC gets issues.xlsx and latest JSON
        imgc_sent = False
        if send_emails and self.recipients.get('IMGC'):
            print(f"\n📤 Sending email to IMGC ({self.recipients['IMGC']})...")
            json_candidates = []
            try:
                extraction_dir = os.path.dirname(str(self.extraction_file))
                json_candidates = glob.glob(os.path.join(extraction_dir, 'final_json_format_*.json'))
                if not json_candidates:
                    json_candidates = glob.glob(os.path.join(extraction_dir, 'pas_field_map_*.json'))
            except Exception:
                json_candidates = []

            latest_json = max(json_candidates, key=os.path.getmtime) if json_candidates else None
            attachments = [issues_attachment]
            if latest_json:
                if os.path.basename(latest_json).lower().startswith('final_json_format_'):
                    attachments.append(_ensure_txt_copy_for_attachment(latest_json))
                else:
                    attachments.append(latest_json)
            imgc_sent = self.send_email(
                to_email=self.recipients['IMGC'],
                subject=imgc_email['subject'],
                body=imgc_email['body'],
                attachment_paths=attachments
            )
        
        # Summary
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"✅ ABHL Email: {abhl_file}")
        if issues_attachment:
            print(f"   📎 Attachment: {issues_attachment}")
        if send_emails:
            print(f"   {'✅ Sent' if abhl_sent else '❌ Not sent'} to {self.recipients.get('ABHL', 'N/A')}")
        print(f"✅ IMGC Email: {imgc_file}")
        if send_emails:
            print(f"   {'✅ Sent' if imgc_sent else '❌ Not sent'} to {self.recipients.get('IMGC', 'N/A')}")
        print(f"   📎 Attachment: {issues_attachment}")
        print("="*80 + "\n")
        
        return {
            'abhl_email': abhl_email,
            'imgc_email': imgc_email,
            'abhl_file': abhl_file,
            'imgc_file': imgc_file,
            'abhl_attachment': issues_attachment,            
            'abhl_sent': abhl_sent,
            'imgc_sent': imgc_sent
        }

    def _extract_loan_id(self):
        """
        Extracts the loan ID from the extraction file path or from the merged DataFrame.
        Returns 'Unknown' if not found.
        """
        import re
        # Try to extract from extraction_file path
        if hasattr(self, 'extraction_file'):
            match = re.search(r'(\d{9,})', str(self.extraction_file))
            if match:
                return match.group(1)
        # Try to extract from merged_df
        if hasattr(self, 'merged_df') and 'Loan ID' in self.merged_df.columns:
            return str(self.merged_df['Loan ID'].iloc[0])
        return "Unknown"

def main():
    import sys
    from pathlib import Path

    # Use the current working directory as the output folder
    output_folder = Path.cwd()
    extraction_file = output_folder / "extraction_results.xlsx"

    print(f"[LOG] Processing only current folder: {output_folder}")
    if extraction_file.exists():
        process_extraction_results(extraction_file, output_folder)
    else:
        print(f"[ERROR] extraction_results.xlsx not found in {output_folder}")

def process_extraction_results(extraction_file, output_folder):
    import pandas as pd
    from dotenv import load_dotenv
    load_dotenv()
    print(f"[LOG] process_extraction_results called for: {extraction_file}")
    config_file = 'FieldConfigrationFile.xlsx'
    recipients_config = os.path.join(output_folder, "abhl_imgc.json")
    smtp_config = "config.json"
    api_key = os.getenv("OPENAI_API_KEY", "")
    print(f"[LOG] Instantiating CompleteEmailGenerator")
    generator = CompleteEmailGenerator(
        extraction_file=extraction_file,
        config_file=config_file,
        api_key=api_key,
        smtp_config=smtp_config
    )
    print(f"[LOG] Recipients loaded: {generator.recipients}")
    generator.generate_and_send_all_emails(output_folder, send_emails=True)
    print(f"[LOG] Email generation and sending complete for: {extraction_file}")

if __name__ == "__main__":
    main()
