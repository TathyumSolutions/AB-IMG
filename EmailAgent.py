#!/usr/bin/env python3
"""
Email Agent - Windows Compatible Version
Monitors email inbox and downloads attachments based on subject filters
Creates a unique folder per matching email based on a Loan ID in the subject.
Saves attachments and email metadata (subject, mail ID) into that folder.
"""

import imaplib
import email
from email.header import decode_header
import os
import time
import logging
from datetime import datetime
from pathlib import Path
import json
import argparse
import sys
import re  # Added for regular expression operations


class EmailAgent:
    """Agent that monitors email and downloads attachments"""

    def __init__(self, config):
        """
        Initialize the email agent

        Args:
            config (dict): Configuration dictionary with email and agent settings
        """
        self.email_address = config['email']['address']
        self.password = config['email']['password']
        self.imap_server = config['email']['imap_server']
        self.imap_port = config['email'].get('imap_port', 993)

        self.target_subjects = config['agent']['target_subjects']
        if isinstance(self.target_subjects, str):
            self.target_subjects = [self.target_subjects]

        # New config property for Loan ID extraction
        self.loan_id_pattern = config['agent']['loan_id_pattern']

        self.save_location = Path(config['agent']['save_location'])
        self.check_interval = config['agent'].get('check_interval', 60)
        self.mark_as_read = config['agent'].get('mark_as_read', False)
        self.only_unseen = config['agent'].get('only_unseen', True)

        self.processed_emails = set()
        self.mail = None

        # Setup logging
        self._setup_logging(config['agent'].get('log_file'))

        # Create save location
        self.save_location.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Root save location: {self.save_location}")

    def _setup_logging(self, log_file=None):
        """Setup logging configuration with Windows-compatible encoding"""
        self.logger = logging.getLogger('EmailAgent')
        self.logger.setLevel(logging.INFO)

        # Console handler with UTF-8 encoding for Windows
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        # Try to set UTF-8 encoding on Windows
        if sys.platform == 'win32':
            try:
                sys.stdout.reconfigure(encoding='utf-8')
            except Exception:
                pass

        console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                           datefmt='%H:%M:%S')
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)

        # File handler (optional) - always use UTF-8
        if log_file:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_format)
            self.logger.addHandler(file_handler)

    def connect(self, retry_count=3, retry_delay=5):
        """
        Connect to the email server with retry logic

        Args:
            retry_count (int): Number of connection attempts
            retry_delay (int): Delay between retries in seconds

        Returns:
            bool: True if connected successfully
        """
        for attempt in range(retry_count):
            try:
                self.logger.info(f"Connecting to {self.imap_server}:{self.imap_port}...")
                self.mail = imaplib.IMAP4_SSL(self.imap_server, self.imap_port)
                self.mail.login(self.email_address, self.password)
                self.logger.info(f"[SUCCESS] Connected to {self.imap_server}")
                return True
            except imaplib.IMAP4.error as e:
                self.logger.error(f"[AUTH FAILED] {e}")
                self.logger.error("Check your email/password. For Gmail, use an App Password!")
                return False
            except Exception as e:
                self.logger.warning(f"Connection attempt {attempt + 1}/{retry_count} failed: {e}")
                if attempt < retry_count - 1:
                    time.sleep(retry_delay)

        self.logger.error("[FAILED] Could not connect after all retries")
        return False

    def disconnect(self):
        """Safely disconnect from email server"""
        if self.mail:
            try:
                self.mail.close()
                self.mail.logout()
                self.logger.info("[DISCONNECT] Logged out from server")
            except Exception as e:
                self.logger.warning(f"Error during disconnect: {e}")

    def decode_subject(self, subject):
        """Decode email subject handling various encodings"""
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
        """Check if subject matches any target subjects"""
        subject_lower = subject.lower()
        return any(target.lower() in subject_lower for target in self.target_subjects)

    def extract_loan_id(self, subject):
        """
        Extract Loan ID from the subject using the configured regex pattern.
        Returns cleaned Loan ID or None.
        """
        match = re.search(self.loan_id_pattern, subject)
        if match:
            # Group 1 should contain the ID
            loan_id = match.group(1).strip()
            # Clean and sanitize the folder name
            invalid_chars = '<>:"|?*\\/\0'
            for char in invalid_chars:
                loan_id = loan_id.replace(char, '_')
            return loan_id
        return None

    def clean_filename(self, filename):
        """Clean and sanitize filename"""
        # Remove or replace invalid characters
        invalid_chars = '<>:"|?*\\/\0'
        for char in invalid_chars:
            filename = filename.replace(char, '_')

        # Limit length
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

            # Add timestamp to avoid overwriting
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            name, ext = os.path.splitext(filename)
            unique_filename = f"{name}_{timestamp}{ext}"

            # Save in the sub-folder
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
        """
        Save mail_subject.txt, mail_body.txt, log.json, and a separate sender/receiver JSON in the sub_folder
        """
        try:
            # Save Subject
            subject_path = sub_folder / "mail_subject.txt"
            with open(subject_path, 'w', encoding='utf-8') as f:
                f.write(subject)
            self.logger.info(f"  [SAVED METADATA] Subject file")

            # Save Mail Body
            body_path = sub_folder / "mail_body.txt"
            with open(body_path, 'w', encoding='utf-8') as f:
                f.write(mail_body)
            self.logger.info(f"  [SAVED METADATA] Mail body file")

            # Save structured log
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
            self.logger.info(f"  [SAVED METADATA] Structured log file")

            # Save sender/receiver JSON as abhl_imgc.json
            abhl_imgc_path = sub_folder / "abhl_imgc.json"
            abhl_imgc_data = {
                "ABHL": from_addr,
                "IMGC": self.email_address
            }
            with open(abhl_imgc_path, 'w', encoding='utf-8') as f:
                json.dump(abhl_imgc_data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"  [SAVED METADATA] abhl_imgc.json file")

        except Exception as e:
            self.logger.error(f"  [ERROR] Failed to save metadata: {e}")

    def get_mail_body(self, msg):
        """Extract the body text from an email message"""
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
        """
        Process a single email message

        Args:
            email_id: Email ID (bytes)
            msg: Email message object

        Returns:
            list: List of saved attachment filepaths
        """
        attachments_saved = []

        try:
            # 1. Get subject and check if it matches target subjects
            subject = self.decode_subject(msg.get('subject', ''))

            if not self.matches_subject(subject):
                return attachments_saved

            # 2. Extract Loan ID for folder naming
            loan_id = self.extract_loan_id(subject)

            if not loan_id:
                self.logger.warning(
                    f"\n[MATCH FOUND] Subject matches, but **could not extract Loan ID** for folder creation. Skipping...")
                self.logger.warning(f"  Subject: {subject}")
                return attachments_saved  # Skip if no Loan ID found

            from_addr = msg.get('from', 'Unknown')
            date = msg.get('date', 'Unknown')

            # 3. Create Loan ID folder and timestamped sub-folder
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

            # Extract mail body
            mail_body = self.get_mail_body(msg)

            # 5. Process attachments
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

            # 4. Save metadata files (after attachments so we can log their names)
            self._save_metadata(sub_folder, subject, mail_body, from_addr, date, loan_id, attachments_saved)

            # 6. Mark as read if configured
            if self.mark_as_read and attachments_saved:
                try:
                    self.mail.store(email_id, '+FLAGS', '\\Seen')
                except Exception as e:
                    self.logger.warning(f"  Could not mark email as read: {e}")

        except Exception as e:
            self.logger.error(f"[ERROR] Processing email: {e}")

        return attachments_saved

    def check_emails(self):
        """
        Check for new emails with target subjects

        Returns:
            list: List of saved attachment filepaths
        """
        all_attachments = []

        try:
            # Reconnect if connection lost
            try:
                self.mail.select('inbox')
            except Exception:
                self.logger.warning("Connection lost, reconnecting...")
                if not self.connect():
                    return all_attachments
                self.mail.select('inbox')

            # Search for emails
            search_criteria = 'UNSEEN' if self.only_unseen else 'ALL'
            status, messages = self.mail.search(None, search_criteria)

            if status != 'OK':
                self.logger.error(f"Search failed: {status}")
                return all_attachments

            email_ids = messages[0].split()

            for email_id in email_ids:
                # email_id is in bytes (e.g., b'123')
                # Skip if already processed
                if email_id in self.processed_emails:
                    continue

                try:
                    # Fetch the email
                    status, msg_data = self.mail.fetch(email_id, '(RFC822)')

                    if status != 'OK':
                        continue

                    for response_part in msg_data:
                        if isinstance(response_part, tuple):
                            msg = email.message_from_bytes(response_part[1])
                            # Pass the bytes ID to process_email
                            attachments = self.process_email(email_id, msg)
                            all_attachments.extend(attachments)

                    # Mark as processed
                    self.processed_emails.add(email_id)

                except Exception as e:
                    self.logger.error(f"[ERROR] Fetching email {email_id.decode('utf-8')}: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"[ERROR] Checking emails: {e}")

        return all_attachments

    def run(self):
        """Run the agent continuously"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("EMAIL AGENT STARTED")
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
                    self.logger.info(
                        f"[SUCCESS] Downloaded {len(attachments)} attachment(s) into {len(set(Path(f).parent for f in attachments))} folder(s)")
                else:
                    self.logger.info("  No new attachments")

                time.sleep(self.check_interval)

        except KeyboardInterrupt:
            self.logger.info("\n\n[STOPPED] Agent stopped by user")
        except Exception as e:
            self.logger.error(f"\n\n[CRASH] Agent crashed: {e}")
        finally:
            self.disconnect()


def load_config(config_file):
    """Load configuration from JSON file"""
    with open(config_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def create_default_config(output_file='config.json'):
    """Create a default configuration file"""
    config = {
        "email": {
            "address": "your.email@gmail.com",
            "password": "your_app_password_here",
            "imap_server": "imap.gmail.com",
            "imap_port": 993
        },
        "agent": {
            "target_subjects": ["Loan Document"],
            "loan_id_pattern": "Loan ID:\\s*([a-zA-Z0-9-]+)",
            "save_location": "./email_attachments",
            "check_interval": 60,
            "mark_as_read": False,
            "only_unseen": True,
            "log_file": "email_agent.log"
        }
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4)

    print(f"[SUCCESS] Created default config: {output_file}")
    print("Please edit this file with your email settings.")
    print("\nFor Gmail:")
    print("  1. Enable 2-Factor Authentication")
    print("  2. Go to: https://myaccount.google.com/apppasswords")
    print("  3. Generate an App Password for 'Mail'")
    print("  4. Use that 16-character password in config.json")
    print("\nLoan ID Pattern:")
    print("  The 'loan_id_pattern' is a Regular Expression used to find the ID in the subject.")
    print(
        "  The ID you want to use as the folder name MUST be in the first capturing group (the first set of parentheses).")
    print("  Example: 'Loan ID: ABC-12345' -> Pattern: 'Loan ID:\\s*([a-zA-Z0-9-]+)'")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Email Agent - Automatic Attachment Downloader')
    parser.add_argument('-c', '--config', default='config.json',
                        help='Configuration file (default: config.json)')
    parser.add_argument('--create-config', action='store_true',
                        help='Create a default configuration file')
    parser.add_argument('--test-connection', action='store_true',
                        help='Test email connection and exit')

    args = parser.parse_args()

    if args.create_config:
        create_default_config(args.config)
        return

    if not os.path.exists(args.config):
        print(f"[ERROR] Config file not found: {args.config}")
        print(f"Run with --create-config to create a default config file")
        return

    try:
        config = load_config(args.config)

        if args.test_connection:
            print("\n[TEST] Testing email connection...")
            agent = EmailAgent(config)
            if agent.connect():
                print("[SUCCESS] Connection test passed!")
                agent.disconnect()
            else:
                print("[FAILED] Connection test failed!")
            return

        agent = EmailAgent(config)
        agent.run()
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()