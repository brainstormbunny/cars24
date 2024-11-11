import sys
import smtplib,email,email.encoders,email.mime.text,email.mime.base
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os
from email.mime.text import MIMEText

def create_message_with_attachment(to, subject, message_text, files):
    SENDER = 'sahil.5@cars24.com'
    SENDERNAME = 'Sahil'

    RECIPIENT = to

    USERNAME_SMTP = "sahil.5@cars24.com"
    PASSWORD_SMTP = "mgbe zjqj lrnt qgoo"
    HOST = "smtp.gmail.com"
    PORT = 587
    SUBJECT = subject

    BODY_TEXT = ("Amazon SES Test\r\n"
                 "This email was sent through the Amazon SES SMTP "
                 "Interface using the Python smtplib package."
                 )

    BODY_HTML = message_text

    msg = MIMEMultipart('alternative')
    msg['Subject'] = SUBJECT
    msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
    msg['To'] = ', '.join(RECIPIENT)

    part1 = MIMEText(BODY_TEXT, 'plain')
    part2 = MIMEText(BODY_HTML, 'html')

    msg.attach(part1)
    msg.attach(part2)

    with open(files, 'rb') as f:
        part = MIMEApplication(f.read())
        part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(files))
        msg.attach(part)

    try:
        server = smtplib.SMTP(HOST, PORT)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(USERNAME_SMTP, PASSWORD_SMTP)
        server.sendmail(SENDER, RECIPIENT, msg.as_string())
        server.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno), e
    else:
        print("Email sent!")

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import email, os, sys

def create_message_without_attachment(to, subject, message_text):
    SENDER = 'sahil.5@cars24.com'
    SENDERNAME = 'Sahil'

    RECIPIENT = to

    USERNAME_SMTP = "sahil.5@cars24.com"
    PASSWORD_SMTP = "mgbe zjqj lrnt qgoo"
    HOST = "smtp.gmail.com"
    PORT = 587
    SUBJECT = subject

    BODY_TEXT = message_text
    BODY_HTML = message_text

    msg = MIMEMultipart('alternative')
    msg['Subject'] = SUBJECT
    msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
    msg['To'] = ', '.join(RECIPIENT)

    part1 = MIMEText(BODY_TEXT, 'plain')
    part2 = MIMEText(BODY_HTML, 'html')

    msg.attach(part1)
    msg.attach(part2)

    try:  
        server = smtplib.SMTP(HOST, PORT)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(USERNAME_SMTP, PASSWORD_SMTP)
        server.sendmail(SENDER, RECIPIENT, msg.as_string())
        server.close()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno), e
    else:
        print ("Email sent!")



def email_with_attachment_multiple_files(to,subject,message_text,files,count):
		# #----------------------------------------------------------------------ENTER THE EMAIL ID HERE------------------------------------------------------------------
	SENDER = 'sahil.5@cars24.com'  
	SENDERNAME = 'autobot '
	
	RECIPIENT  = to
	
	USERNAME_SMTP = "AKIAXWY47RBV43MJ3JH4"

	PASSWORD_SMTP = "mgbe zjqj lrnt qgoo"

	HOST = "smtp.gmail.com"
	PORT = 587

	SUBJECT = subject

	BODY_TEXT = ("Amazon SES Test\r\n"
	             "This email was sent through the Amazon SES SMTP "
	             "Interface using the Python smtplib package."
	            )

	BODY_HTML = message_text

	msg = MIMEMultipart('alternative')
	msg['Subject'] = SUBJECT
	msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
	msg['To'] = ', '.join(RECIPIENT)

	
	part1 = MIMEText(BODY_TEXT, 'plain')
	part2 = MIMEText(BODY_HTML, 'html')

	msg.attach(part1)
	msg.attach(part2)
	for i in range(len(files)):
		with open(files[i], 'rb') as f:
			part = MIMEApplication(f.read())
			part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(files[i]))
			msg.attach(part)


	try:  
	    server = smtplib.SMTP(HOST, PORT)
	    server.ehlo()
	    server.starttls()
	    server.ehlo()
	    server.login(USERNAME_SMTP, PASSWORD_SMTP)
	    server.sendmail(SENDER, RECIPIENT, msg.as_string())
	    server.close()
	except Exception as e:
	    exc_type, exc_obj, exc_tb = sys.exc_info()
	    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
	    print(exc_type, fname, exc_tb.tb_lineno), e
	else:
	    print ("Email sent!")
















# def cod(to,subject,message_text):
# 	SENDER = 'no-reply@magicpin.in'  
# 	SENDERNAME = 'magicpin'

# 	RECIPIENT  = to

# 	USERNAME_SMTP = "AKIAXWY47RBV43MJ3JH4"
# 	PASSWORD_SMTP = "BA8ooVX046wzwhdQLVsCY2r8OaabN8ZuvMaSeycAPEQZ"
# 	HOST = "smtp.gmail.com"
# 	PORT = 587
# 	SUBJECT = subject

# 	BODY_TEXT = ("Amazon SES Test\r\n"
# 				"This email was sent through the Amazon SES SMTP "
# 				"Interface using the Python smtplib package."
# 				)

# 	BODY_HTML = message_text
	
# 	msg = MIMEMultipart('alternative')
# 	msg['Subject'] = SUBJECT
# 	msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
# 	msg['To'] = ', '.join(RECIPIENT)

# 	part1 = MIMEText(BODY_TEXT, 'plain')
# 	part2 = MIMEText(BODY_HTML, 'html')

# 	msg.attach(part1)
# 	msg.attach(part2)

# 	try:  
# 		server = smtplib.SMTP(HOST, PORT)
# 		server.ehlo()
# 		server.starttls()
# 		server.ehlo()
# 		server.login(USERNAME_SMTP, PASSWORD_SMTP)
# 		server.sendmail(SENDER, RECIPIENT, msg.as_string())
# 		server.close()
# 	except Exception as e:
# 		exc_type, exc_obj, exc_tb = sys.exc_info()
# 		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
# 		print(exc_type, fname, exc_tb.tb_lineno), e
# 	else:
# 		print ("Email sent!")