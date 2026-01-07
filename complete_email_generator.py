"""
Complete Email Generator - All Features in One File (WITH HTML CLEANING)
====================================================
Features:
- Data cleaning (whitespace, special chars, case-insensitive)
- First/Second preference support
- Major field error messages
- Email sending via SMTP with HTML support
- GPT integration (optional)
- ABHL email now includes Excel attachment with high criticality issues
- **NEW: HTML cleaning for readable email formatting**
"""

import pandas as pd
import os
import json
import smtplib
import shutil
from openai import OpenAI, AzureOpenAI
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from bs4 import BeautifulSoup
import re
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


def clean_html_email(html_content):
    """
    Clean messy HTML while preserving tables and important formatting
    Makes email human-readable by removing excessive inline styles and nested divs
    """
    if not html_content:
        return ""
    
    print("[LOG] Cleaning HTML content for readability...")
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove script and style tags completely
    for tag in soup(['script', 'style', 'meta', 'link']):
        tag.decompose()
    
    # Clean up inline styles - keep only essential ones
    for tag in soup.find_all(True):
        if tag.has_attr('style'):
            style = tag['style']
            # Keep only essential styles for tables
            if tag.name in ['table', 'td', 'th', 'tr']:
                # Keep border, padding, background for tables
                essential_styles = []
                for prop in ['border', 'padding', 'background-color', 'color', 'text-align', 'width']:
                    if prop in style.lower():
                        # Extract this property
                        match = re.search(rf'{prop}\s*:\s*[^;]+', style, re.IGNORECASE)
                        if match:
                            essential_styles.append(match.group(0))
                
                if essential_styles:
                    tag['style'] = '; '.join(essential_styles)
                else:
                    del tag['style']
            else:
                # For non-table elements, remove most inline styles
                del tag['style']
        
        # Remove unnecessary attributes
        attrs_to_remove = ['class', 'id', 'dir']
        for attr in list(tag.attrs.keys()):
            if attr in attrs_to_remove or attr.startswith('data-'):
                del tag.attrs[attr]
    
    # Remove empty tags (but keep br, hr, img)
    for tag in soup.find_all():
        if len(tag.get_text(strip=True)) == 0 and tag.name not in ['br', 'hr', 'img']:
            tag.decompose()
    
    # Get clean HTML
    clean_html = str(soup)
    
    # Remove excessive whitespace
    clean_html = re.sub(r'\n\s*\n', '\n', clean_html)
    
    print("[LOG] HTML cleaned successfully")
    return clean_html


def extract_readable_text(html_content):
    """
    Extract plain text in readable format from HTML
    """
    if not html_content:
        return ""
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove script and style tags
    for tag in soup(['script', 'style']):
        tag.decompose()
    
    # Get text with proper spacing
    text = soup.get_text(separator='\n', strip=True)
    
    # Clean up excessive newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    return text


class CompleteEmailGenerator:
    """Complete email generator with all features including HTML cleaning"""
    
    def __init__(self, extraction_file, config_file, api_key, smtp_config, folder_path):
        print(f"[LOG] Initializing CompleteEmailGenerator")
        self.extraction_file = extraction_file
        self.config_file = config_file
        self.api_key = api_key
        self.smtp_config = self.load_smtp_config(smtp_config)
        print(f"[LOG] Loading recipients from {smtp_config}")
        self.recipients = self.load_recipients_from_config(smtp_config, folder_path)
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

    def load_recipients_from_config(self, config_path, folder_path):
        import os
        import json
        try:
            # Load config for IMGC email
            with open(config_path, "r") as f:
                config = json.load(f)
                imgc = config.get("abhl_imgc", {}).get("imgc_email_id", "")

            # Find email_metadata.json in the folder_path
            metadata_path = os.path.join(folder_path, "email_metadata.json")
            abhl_list = []
            if os.path.exists(metadata_path):
                with open(metadata_path, "r") as meta_f:
                    metadata = json.load(meta_f)
                    from_field = metadata.get("from", "")
                    # Extract email from "from" field (format: "Name <email>")
                    import re
                    match = re.search(r'<([^>]+)>', from_field)
                    if match:
                        abhl_list.append(match.group(1))
                    else:
                        abhl_list.append(from_field)  # fallback: use as is

            return {"ABHL": abhl_list, "IMGC": imgc}
        except Exception as e:
            print(f"[ERROR] Could not load recipients from config: {e}")
            return {"ABHL": [], "IMGC": ""}

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
            return ""
        if isinstance(val, float) and pd.isna(val):
            return ""
        cleaned = str(val).strip().lower()
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

    def send_email(self, to_email, subject, body, attachment_path=None, attachment_paths=None, is_html=False):
        """
        Send email with support for both plain text and HTML
        
        Args:
            to_email: Recipient email(s)
            subject: Email subject
            body: Email body (plain text or HTML)
            attachment_path: Single attachment path (deprecated, use attachment_paths)
            attachment_paths: List of attachment paths
            is_html: If True, send body as HTML, else as plain text
        """
        print(f"[LOG] Attempting to send email to: {to_email}")
        if not self.smtp_config:
            print("[ERROR] SMTP config not loaded. Cannot send email.")
            return False
        try:
            msg = MIMEMultipart('alternative')  # Changed to 'alternative' to support both text and HTML
            msg['From'] = self.smtp_config['address']
            if isinstance(to_email, (list, tuple)):
                msg['To'] = ', '.join(to_email)
                recipients = list(to_email)
            else:
                msg['To'] = to_email
                recipients = [to_email]
            msg['Subject'] = subject
            
            # Attach body as HTML or plain text
            if is_html:
                # Create plain text version for email clients that don't support HTML
                plain_text = extract_readable_text(body)
                msg.attach(MIMEText(plain_text, 'plain', 'utf-8'))
                msg.attach(MIMEText(body, 'html', 'utf-8'))
                print("[LOG] Email body attached as HTML with plain text fallback")
            else:
                msg.attach(MIMEText(body, 'plain'))
                print("[LOG] Email body attached as plain text")
            
            # Handle attachments
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
            # llm_logger.info(json.dumps({
            #     "model": "gpt-4o-mini",
            #     "input_tokens": completion.usage.prompt_tokens,
            #     "output_tokens": completion.usage.completion_tokens,
            #     "prompt": prompt,
            #     "response": result
            # }))
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

    def create_issues_excel(self, output_folder):
        """Create an Excel file with all rows, filtered columns (for both ABHL and IMGC)."""
        print(f"[LOG] Creating issues Excel file for both ABHL and IMGC")
        # Load config to get all columns
        config_df = pd.read_excel(self.config_file)
        all_columns = config_df.columns.tolist()

        # Columns to exclude
        exclude_keywords = ['Data Type', 'Field length', 'Primary Source Document', 'Secondary Source Document']
        exclude_columns = [col for col in all_columns if any(key in col for key in exclude_keywords) or 'Description' in col]
        print(f"[LOG] Columns to exclude: {exclude_columns}")
        mismatch_extraction_file = os.path.join(output_folder, "extraction_results_with_mismatch.xlsx")
        merged_df_1 = pd.read_excel(mismatch_extraction_file)
        print(f"[LOG] Merged DataFrame shape before filtering: {merged_df_1.shape}")
        print(f"[LOG] First row of merged columns: {merged_df_1.head(1)}")
        # Columns to keep
        keep_columns = [col for col in merged_df_1.columns if col not in exclude_columns]
        print(f"[LOG] Columns to keep: {keep_columns}")
        # Filter DataFrame
        filtered_df = merged_df_1[keep_columns]

        # Create filename
        filename = os.path.join(output_folder, "final_extracted_output.xlsx")
        filtered_df.to_excel(filename, index=False)
        print(f"[LOG] Issues Excel file created: {filename}")
        return filename

    def _get_document_names_from_mapping(self, mapping_json_path):
        with open(mapping_json_path, 'r') as f:
            mapping = json.load(f)
        return list(mapping.keys())

    def generate_abhl_email(self, mapping_json_path, original_subject=None, original_body=None, original_sender=None, original_date=None):
        """
        Generate ABHL email with cleaned HTML from original message
        
        KEY CHANGE: Clean the original_body HTML to make it human-readable
        """
        print(f"[LOG] Generating ABHL email (high criticality issues)")
        print("[LOG] Loading major issues for ABHL email mapping JSON ", mapping_json_path)
        major_issues = self.get_major_issues()
        loan_id = self._extract_loan_id()

        doc_names = self._get_document_names_from_mapping(mapping_json_path)
        filtered_docs = [
            doc for doc in doc_names
            if "mail_subject.txt" not in doc and "mail_body.txt" not in doc
        ]
        num_docs = len(filtered_docs)
        doc_list = '\n'.join([f"• {doc}" for doc in filtered_docs])

        total_captured_fields = 0
        if 'Final Data for PAS System' in self.merged_df.columns:
            final_data_col = self.merged_df['Final Data for PAS System']
            valid_mask = final_data_col.notna() & final_data_col.astype(str).str.strip().ne('')
            total_captured_fields = int(valid_mask.sum())

        # Compose the new content
        if major_issues.empty:
            new_content_lines = [
                "Dear ABHFL Team,",
                "I hope this email finds you well.",
                "",
                "We are pleased to inform you that the documents shared to initiate the loan application have been successfully processed through our data extraction and quality check workflow.",
                "",
                "<strong>Quality Check Summary:</strong>",
                "• No discrepancies were identified",
                "• Data values are consistent across the submitted documents",
                "",
                "At this stage, no additional information or revised documents are required. However, we will keep you informed for any further inputs be needed during subsequent processing.",
                "",
                "<strong>Documents Processed</strong>",
                f"A total of {num_docs} documents were received and processed, including:",
                f"{doc_list}",
                "",
                "If you require any additional information or clarification, please feel free to reach out to us.",
                "Warm regards,",
                "IMGC Team",
                "________________________________________",
                "This is a system-generated email. Please do not reply to this message.",
                "________________________________________",
                "For Implementation Use Only"
            ]
            new_content_html = '<br>'.join(new_content_lines)
        else:
            summary_lines = [
                "Dear ABHFL Team,",
                "I hope this email finds you well.",
                "",
                "We have processed your documents and below is the document captured summary",
                "",
                f"<strong>Total Fields Captured:</strong> {total_captured_fields}",
                "",
                "<strong>Documents Processed</strong>",
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
            new_content_html = '<br>'.join(summary_lines)

        # **KEY CHANGE**: Clean the original HTML body for readability
        cleaned_original_body = ""
        if original_body:
            print("[LOG] Cleaning original email HTML for better readability...")
            cleaned_original_body = clean_html_email(original_body)

        # Compose the complete HTML email with cleaned original message
        complete_html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{
            font-family: Arial, Helvetica, sans-serif;
            font-size: 14px;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 15px 0;
            border: 1px solid #ddd;
        }}
        
        th, td {{
            border: 1px solid #ddd;
            padding: 10px;
            text-align: left;
        }}
        
        th {{
            background-color: #f2f2f2;
            font-weight: bold;
        }}
        
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        
        blockquote {{
            margin: 10px 0;
            padding-left: 15px;
            border-left: 3px solid #ccc;
            color: #666;
        }}
        
        .gmail_quote {{
            margin: 10px 0;
            padding-left: 15px;
            border-left: 3px solid #ccc;
            color: #666;
        }}
        
        hr {{
            border: none;
            border-top: 1px solid #ccc;
            margin: 20px 0;
        }}
        
        p {{
            margin: 10px 0;
        }}
        
        strong, b {{
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <!-- New reply content -->
    <div class="reply-content">
        {new_content_html}
    </div>
    
    <!-- Divider -->
    <hr>
    
    <!-- Original message header -->
    {f'<div style="color: #666; margin: 15px 0;"><strong>On {original_date or "[date]"}, {original_sender or "[sender]"} wrote:</strong></div>' if (original_date or original_sender) else ''}
    
    <!-- Quoted original message (CLEANED) -->
    <blockquote class="gmail_quote">
        {cleaned_original_body}
    </blockquote>
</body>
</html>
"""

        # Set subject as a reply
        if original_subject:
            subject_hint = f"Re: {original_subject}"
        else:
            subject_hint = f"Loan ID: {loan_id} Loan Application Document Processing Update"

        email = {
            'subject': subject_hint,
            'body': complete_html,
            'is_html': True  # Flag to indicate this is HTML content
        }
        print(f"[LOG] ABHL reply email generated with cleaned HTML")
        return email

    def generate_imgc_email(self, mapping_json_path):
        print(f"[LOG] Generating IMGC email (criticality analysis)")
        loan_id = self._extract_loan_id()
        print("IMGC Loan Id:", loan_id)
        doc_names = self._get_document_names_from_mapping(mapping_json_path)
        filtered_docs = [
            doc for doc in doc_names
            if "mail_subject.txt" not in doc and "mail_body.txt" not in doc
        ]
        num_docs = len(filtered_docs)
        print("IMGC JSON path:", mapping_json_path)
        print("IMGC doc_names:", doc_names)
        doc_list = '\n'.join([f"• {doc}" for doc in filtered_docs])

        # Extraction statistics
        total_fields = len(self.merged_df)
        all_issues = self.identify_issues()
        total_issues = len(all_issues)
        high_issues = all_issues[all_issues['Criticality'].astype(str).str.upper() == 'HIGH']
        low_issues = all_issues[all_issues['Criticality'].astype(str).str.upper() != 'HIGH']
        high_count = len(high_issues)
        low_count = len(low_issues)

        total_captured_fields = 0
        if 'Final Data for PAS System' in self.merged_df.columns:
            final_data_col = self.merged_df['Final Data for PAS System']
            valid_mask = final_data_col.notna() & final_data_col.astype(str).str.strip().ne('')
            total_captured_fields = int(valid_mask.sum())

        subject = f"ABHFL – Loan ID: {loan_id} – Document Data Extraction Report"
        body = f"""Dear IMGC Team,

I hope you are doing well.
A total of {num_docs} loan-related documents were received and successfully processed as part of this request. The documents include:
{doc_list}
Please find below a summary of the data extraction performed on the received documents, including overall extraction statistics:

📊 Data Extraction Summary
• Total Fields Processed: {total_fields}
• Total Fields Captured : {total_captured_fields}

The complete extracted Excel file has been attached for review and audit purposes.
If any clarification, correction, or follow-up action is required, please coordinate internally as per the defined workflow.
________________________________________
This is a system-generated email. Please do not reply to this message.
"""

        email = {
            'subject': subject,
            'body': body,
            'is_html': False  # IMGC email is plain text
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

        # --- Extract original mail fields from files in output_dir ---
        subject_path = os.path.join(output_dir, "mail_subject.txt")
        body_html_path = os.path.join(output_dir, "mail_body.html")
        metadata_path = os.path.join(output_dir, "email_metadata.json")
        original_subject = None
        original_body = None
        original_sender = None
        original_date = None

        if os.path.exists(subject_path):
            with open(subject_path, "r", encoding="utf-8") as f:
                original_subject = f.read().strip()
        if os.path.exists(body_html_path):
            with open(body_html_path, "r", encoding="utf-8") as f:
                original_body = f.read()
        if os.path.exists(metadata_path):
            with open(metadata_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
                original_sender = meta.get("from")
                original_date = meta.get("date")

        # Generate ABHL email as a reply (with cleaned HTML)
        print("📧 Generating ABHL email (High Criticality Issues)...")
        mapping_json = os.path.join(output_dir, f"document_column_mapping.json")
        abhl_email = self.generate_abhl_email(
            mapping_json,
            original_subject=original_subject,
            original_body=original_body,
            original_sender=original_sender,
            original_date=original_date
        )
        abhl_file = f"{output_dir}/email_to_ABHL.html"  # Changed extension to .html
        self.save_email_to_file(abhl_email, abhl_file)

        # Create issues Excel attachment for both ABHL and IMGC
        print("📊 Creating issues Excel file for both ABHL and IMGC...")
        issues_attachment = self.create_issues_excel(output_dir)

        # Send ABHL email with attachment (AS HTML)
        abhl_sent = False
        if send_emails and self.recipients.get('ABHL'):
            print(f"📤 Sending HTML email to ABHL ({self.recipients['ABHL']})...")
            abhl_sent = self.send_email(
                to_email=self.recipients['ABHL'],
                subject=abhl_email['subject'],
                body=abhl_email['body'],
                attachment_path=issues_attachment,
                is_html=abhl_email.get('is_html', False)  # Use HTML flag
            )

        # Generate IMGC email (plain text)
        print("\n📧 Generating IMGC email (Low Criticality Issues)...")
        mapping_json = os.path.join(output_dir, f"document_column_mapping.json")
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
                attachment_paths=attachments,
                is_html=imgc_email.get('is_html', False)  # Use HTML flag (False for IMGC)
            )

        # Summary
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"✅ ABHL Email: {abhl_file} (HTML format with cleaned original message)")
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
        smtp_config=smtp_config,
        folder_path=output_folder
    )
    print(f"[LOG] Recipients loaded: {generator.recipients}")
    generator.generate_and_send_all_emails(output_folder, send_emails=True)
    print(f"[LOG] Email generation and sending complete for: {extraction_file}")


if __name__ == "__main__":
    main()
