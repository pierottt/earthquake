import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def send_test_email():
    try:
        # Email settings from .env
        email_host = os.getenv("EMAIL_HOST")
        email_port = int(os.getenv("EMAIL_PORT"))
        email_user = os.getenv("EMAIL_USER")
        email_pass = os.getenv("EMAIL_PASS")
        email_to = os.getenv("EMAIL_TO")

        # Create the email
        msg = MIMEMultipart()
        msg['From'] = email_user
        msg['To'] = email_to
        msg['Subject'] = "Test Email"
        body = "This is a test email to verify .env configuration."
        msg.attach(MIMEText(body, 'plain'))

        # Connect to the SMTP server and send the email
        with smtplib.SMTP(email_host, email_port) as server:
            server.starttls()  # Secure the connection
            server.login(email_user, email_pass)
            server.sendmail(email_user, email_to, msg.as_string())
            print("Email sent successfully!")

    except Exception as e:
        print(f"Failed to send email: {e}")

if __name__ == "__main__":
    send_test_email()
