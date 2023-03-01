import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header

def sendHtmlMail(emailId, subject, content):
    sender_email = "admin@mlhtracker.com"
    sender_password = "u+YDrM9U5^K{"
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = str(Header('MLH Tracker', 'utf-8')) + '<admin@mlhtracker.com>'  # set sender name and email address"
    msg["To"] = emailId

    # Set the HTML content
    html = content
    msg.attach(MIMEText(html, "html"))

    # Send the email
    with smtplib.SMTP("mail.mlhtracker.com", 587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(sender_email, sender_password)
        smtp.send_message(msg)

def sendNormalMail(emailId, subject, content):
    sender_email = "admin@mlhtracker.com"
    sender_password = "u+YDrM9U5^K{"
    receiver_email = emailId

    server = smtplib.SMTP("mail.mlhtracker.com", 587)
    server.ehlo()
    server.starttls()
    server.login(sender_email, sender_password)

    subject = subject
    message = content

    header = f"From: {sender_email}\nTo: {receiver_email}\nSubject: {subject}\n"
    body = f"{header}\n{message}"

    try :
        server.sendmail(sender_email, receiver_email, body)
    except Exception as e:
        logging.error(str(e))
    server.quit()

sendHtmlMail("kannanaa21@gmail.com", "test", "<p>Kannan</p>")