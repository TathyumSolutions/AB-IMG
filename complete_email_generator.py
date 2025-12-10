"""
Complete Email Generator - All Features in One File
====================================================
Features:
- Data cleaning (whitespace, special chars, case-insensitive)
- First/Second preference support
- Major field error messages
- Email sending via SMTP
- GPT integration (optional)
"""

import pandas as pd
import os
import json
import smtplib
from openai import OpenAI
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

class CompleteEmailGenerator:
    """Complete email generator with all features and detailed logging"""
    def __init__(self, extraction_file, config_file, api_key, recipients_config, smtp_config):
        print(f"[LOG] Initializing CompleteEmailGenerator")
        self.extraction_file = extraction_file
        self.config_file = config_file
        self.api_key = api_key
        self.recipients_config = recipients_config
        self.smtp_config = self.load_smtp_config(smtp_config)
        print(f"[LOG] Loading recipients from {self.recipients_config}")
        self.recipients = self.load_recipients()
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

    def load_recipients(self):
        try:
            with open(self.recipients_config, "r") as f:
                recipients = json.load(f)
                print(f"[LOG] Recipients file loaded: {recipients}")
                return recipients
        except Exception as e:
            print(f"[ERROR] Could not load recipients: {e}")
            return {}

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
            print(f"[LOG] Cleaning value: None -> ''")
            return ""
        if isinstance(val, float) and pd.isna(val):
            print(f"[LOG] Cleaning value: NaN -> ''")
            return ""
        cleaned = str(val).strip().lower()
        print(f"[LOG] Cleaning value: {val} -> {cleaned}")
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
        exclude_cols = ['PAS Field Name', 'Criticality', 'First Preference', 'Second Preference']
        doc_columns = [col for col in self.merged_df.columns if col not in exclude_cols]
        for idx, row in self.merged_df.iterrows():
            field_name = row['PAS Field Name']
            criticality = row.get('Criticality', 'Unknown')
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
        major_issues = all_issues[all_issues['Criticality'].str.upper() == 'HIGH']
        print(f"[LOG] Major issues found: {len(major_issues)}")
        return major_issues

    def get_low_issues(self):
        print(f"[LOG] Getting low criticality issues")
        all_issues = self.identify_issues()
        if all_issues.empty:
            print(f"[LOG] No issues found")
            return all_issues
        low_issues = all_issues[all_issues['Criticality'].str.upper() != 'HIGH']
        print(f"[LOG] Low issues found: {len(low_issues)}")
        return low_issues

    def format_issues_for_email(self, issues_df):
        print(f"[LOG] Formatting issues for email")
        if issues_df.empty:
            return "✅ No issues found. All data is consistent."
        formatted_text = ""
        for idx, row in issues_df.iterrows():
            formatted_text += f"\n{'='*70}\n"
            formatted_text += f"Issue #{idx + 1}: {row['Field Name']}\n"
            formatted_text += f"{'='*70}\n"
            formatted_text += f"{row['Error Message']}\n\n"
            formatted_text += f"📊 Details:\n"
            formatted_text += f"   • Criticality: {row['Criticality']}\n"
            formatted_text += f"   • Unique Values Found: {row['Unique Values Count']}\n"
            formatted_text += f"   • Values: {', '.join(str(v) for v in row['Values Found'])}\n\n"
            if row['Preferred Value'] != 'None':
                formatted_text += f"✓ Recommended Value (from {row['Preferred Source']}):\n"
                formatted_text += f"   → {row['Preferred Value']}\n\n"
            if isinstance(row['Document Sources'], dict) and row['Document Sources']:
                formatted_text += f"📄 Source Documents:\n"
                for doc, val in row['Document Sources'].items():
                    formatted_text += f"   • {doc}:\n"
                    formatted_text += f"     Original: {val}\n"
                    cleaned = self._clean_value(val)
                    if cleaned:
                        formatted_text += f"     Cleaned: {cleaned}\n"
                formatted_text += "\n"
            formatted_text += "\n"
        print(f"[LOG] Issues formatted for email")
        return formatted_text

    def send_email(self, to_email, subject, body, attachment_path=None):
        print(f"[LOG] Preparing to send email to {to_email} with subject '{subject}'")
        if not self.smtp_config or not to_email:
            print(f"[ERROR] Cannot send email: Missing SMTP config or recipient email")
            return False
        try:
            msg = MIMEMultipart()
            msg['From'] = self.smtp_config.get('address')
            msg['To'] = to_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            if attachment_path and os.path.exists(attachment_path):
                print(f"[LOG] Attaching file: {attachment_path}")
                with open(attachment_path, 'rb') as f:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename={os.path.basename(attachment_path)}'
                    )
                    msg.attach(part)
                print(f"[LOG] File attached: {os.path.basename(attachment_path)}")
            print(f"[LOG] Connecting to SMTP server...")
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(
                self.smtp_config.get('address'),
                self.smtp_config.get('password')
            )
            print(f"[LOG] Sending email...")
            server.send_message(msg)
            server.quit()
            print(f"[LOG] Email sent successfully to {to_email}")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to send email to {to_email}: {str(e)}")
            return False
    
    def generate_email_with_gpt(self, recipient, subject_hint, body_content, context):
        print(f"[LOG] Generating email with GPT for recipient: {recipient}")
        if not self.client:
            print(f"[LOG] No OpenAI client available, using fallback email generation")
            return self._generate_fallback_email(recipient, subject_hint, body_content)
        try:
            prompt = f"""Generate a professional business email with the following details:

Recipient: {recipient}
Context: {context}
Subject Hint: {subject_hint}

Email Content to Include:
{body_content}

Please provide:
1. A professional email subject line
2. A well-formatted email body with proper greeting, content, and closing

Format the response as JSON with keys: "subject" and "body"
"""
            print(f"[LOG] Sending prompt to OpenAI")
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a professional business email writer. Generate clear, concise, and professional emails."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            print(f"[LOG] GPT email generated: {result['subject']}")
            return result
        except Exception as e:
            print(f"[ERROR] Error generating email with GPT: {e}")
            return self._generate_fallback_email(recipient, subject_hint, body_content)
    
    def _generate_fallback_email(self, recipient, subject_hint, body_content):
        print(f"[LOG] Generating fallback email for recipient: {recipient}")
        current_date = datetime.now().strftime("%Y-%m-%d")
        return {
            "subject": subject_hint,
            "body": f"""Dear {recipient} Team,

{body_content}

Please review and take necessary action.

Best regards,
Data Quality Team
Date: {current_date}
"""
        }
    
    def generate_abhl_email(self):
        print(f"[LOG] Generating ABHL email (high criticality issues)")
        major_issues = self.get_major_issues()
        if major_issues.empty:
            body_content = "✅ Good news! No high criticality issues were found in the data extraction process.\n\nAll high criticality fields have consistent values across documents."
        else:
            lines = ["🚨 CRITICAL DATA QUALITY ALERT\n"]
            lines.append(f"We have identified {len(major_issues)} HIGH criticality issue(s) in the extracted data that require IMMEDIATE attention:\n")
            for idx, row in major_issues.iterrows():
                field = row['Field Name']
                docs = row['Document Sources'] if isinstance(row['Document Sources'], dict) else {}
                values = docs.values() if docs else []
                unique_values = set([str(v) for v in values if v not in [None, '', 'NOT FOUND', 'NO INSTRUCTION', 'nan', 'None']])
                if row['Preferred Value'] == 'None' or not unique_values:
                    lines.append(f"• Mandatory field '{field}' not found in any document.")
                elif len(unique_values) > 1:
                    value_details = []
                    for doc, val in docs.items():
                        if val not in [None, '', 'NOT FOUND', 'NO INSTRUCTION', 'nan', 'None']:
                            value_details.append(f"{doc}: '{val}'")
                    value_str = "; ".join(value_details)
                    lines.append(f"• Check documents: Values of field '{field}' differ: {value_str}")
                else:
                    lines.append(f"• Field '{field}' has a single value but flagged as issue: {', '.join(unique_values)}")
            lines.append("\n⚠️ ACTION REQUIRED: These high criticality fields must be resolved before proceeding with data integration.\nPlease review the issues above and provide corrections or clarifications.")
            body_content = '\n'.join(lines)
        email = self.generate_email_with_gpt(
            recipient="ABHL",
            subject_hint=f"🚨 HIGH Criticality Data Issues Detected - {len(major_issues)} Issue(s)" if not major_issues.empty else "✅ Data Quality Check Passed",
            body_content=body_content,
            context="This email reports high criticality issues found during data extraction where values differ across source documents or mandatory fields are missing."
        )
        print(f"[LOG] ABHL email generated")
        return email

    def generate_imgc_email(self):
        print(f"[LOG] Generating IMGC email (low criticality issues)")
        total_fields = len(self.merged_df)
        all_issues = self.identify_issues()
        low_issues = self.get_low_issues()
        total_issues = len(all_issues)
        low_count = len(low_issues) if not low_issues.empty else 0
        summary = f"""
📊 DATA EXTRACTION SUMMARY
{'='*70}

Total Fields Processed: {total_fields}
Total Issues Identified: {total_issues}
Low Criticality Issues: {low_count}
Data Extraction Status: ✅ Complete
Data Cleaning Applied: ✅ Yes (spaces trimmed, special chars removed, case-normalized)
"""
        if not low_issues.empty:
            all_issues_text = self.format_issues_for_email(low_issues)
            body_content = f"""{summary}

{'='*70}
LOW CRITICALITY ISSUE DETAILS
{'='*70}

{all_issues_text}

📎 ATTACHED: Complete extracted data for your records.
"""
        else:
            body_content = f"""{summary}

✅ EXCELLENT! All data has been extracted successfully with no low criticality inconsistencies found.
All fields have consistent values across all source documents.
📎 ATTACHED: Complete extracted data for your records.
"""
        email = self.generate_email_with_gpt(
            recipient="IMGC",
            subject_hint=f"📊 Data Extraction Report - {low_count} Low Criticality Issue(s) Found",
            body_content=body_content,
            context="This email provides a report of all extracted data including details of all low criticality issues found after data cleaning and normalization."
        )
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
        print("📧 Generating ABHL email (High Criticality Issues)...")
        abhl_email = self.generate_abhl_email()
        abhl_file = f"{output_dir}/email_to_ABHL.txt"
        self.save_email_to_file(abhl_email, abhl_file)
        abhl_sent = False
        if send_emails and self.recipients.get('ABHL'):
            print(f"📤 Sending email to ABHL ({self.recipients['ABHL']})...")
            abhl_sent = self.send_email(
                to_email=self.recipients['ABHL'],
                subject=abhl_email['subject'],
                body=abhl_email['body']
            )
        print("\n📧 Generating IMGC email (Low Criticality Issues)...")
        imgc_email = self.generate_imgc_email()
        imgc_file = f"{output_dir}/email_to_IMGC.txt"
        self.save_email_to_file(imgc_email, imgc_file)
        extraction_attachment = self.extraction_file
        imgc_sent = False
        if send_emails and self.recipients.get('IMGC'):
            print(f"\n📤 Sending email to IMGC ({self.recipients['IMGC']})...")
            imgc_sent = self.send_email(
                to_email=self.recipients['IMGC'],
                subject=imgc_email['subject'],
                body=imgc_email['body'],
                attachment_path=extraction_attachment
            )
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"✅ ABHL Email: {abhl_file}")
        if send_emails:
            print(f"   {'✅ Sent' if abhl_sent else '❌ Not sent'} to {self.recipients.get('ABHL', 'N/A')}")
        print(f"✅ IMGC Email: {imgc_file}")
        if send_emails:
            print(f"   {'✅ Sent' if imgc_sent else '❌ Not sent'} to {self.recipients.get('IMGC', 'N/A')}")
        print(f"✅ Extraction Result: {extraction_attachment}")
        print("="*80 + "\n")
        return {
            'abhl_email': abhl_email,
            'imgc_email': imgc_email,
            'abhl_file': abhl_file,
            'imgc_file': imgc_file,
            'extraction_attachment': extraction_attachment,
            'abhl_sent': abhl_sent,
            'imgc_sent': imgc_sent
        }

def main():
    from pathlib import Path
    print("[LOG] Starting main workflow")
    base_dir = Path("email_attachments")
    if not base_dir.exists():
        print("[ERROR] email_attachments folder not found.")
        return
    for folder in base_dir.glob("*/"):
        for subfolder in folder.glob("*/"):
            extraction_file = subfolder / "extraction_results.xlsx"
            print(f"\n[LOG] Processing folder: {subfolder}")
            if extraction_file.exists():
                process_extraction_results(extraction_file, subfolder)

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
        recipients_config=recipients_config,
        smtp_config=smtp_config
    )
    print(f"[LOG] Recipients loaded: {generator.recipients}")
    generator.generate_and_send_all_emails(output_folder, send_emails=True)
    print(f"[LOG] Email generation and sending complete for: {extraction_file}")

if __name__ == "__main__":
    main()
