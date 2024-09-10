# This is simple batch to exract data from SQL DB and sent dataset in CSV UTF8 via Email to recepients
# Recepient's list from command line arguments

import sqlalchemy as sa
import pandas as pd
import datetime as dt
import sys

def get_date_time(next_month_date) :
  import datetime as _dt
  
  first_time = '00:00:00.000'
  last_time = '23:59:59.000'
  first_date = next_month_date

  last_date = first_date.date().replace(day=1) - _dt.timedelta(days=1)
  first_date = last_date.replace(day=1)
  return first_date.strftime('%Y-%m-%d ') + first_time, last_date.strftime('%Y-%m-%d ') + last_time


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.mime.text import MIMEText

def send_email(send_from, send_to, subject, file_name, server='156.147.1.150') :
  msg = MIMEMultipart()
  msg['Subject'] = out_file_name
  msg['From'] = send_from
  msg['To'] = ','.join(send_to)

  msg.attach(MIMEText(file_name, "plain"))

  part = MIMEBase('application', 'octet-stream')
  part.set_payload(open(file_name, 'rb').read())

  encoders.encode_base64(part)
    
  part.add_header('Content-Disposition', f'attachment; filename={file_name}')

  msg.attach(part)

  server = smtplib.SMTP(server)
  server.sendmail(send_from, send_to, msg.as_string())

if (len(sys.argv) - 1) > 0 :
  SERVER = '172.26.74.116'
  DATABASE = '1CGERP'
  USERNAME = 'svcmq_connector'
  PASSWORD = 'b2d5c95ab42e2adc4284ae697B4E99EF'

  connectionString =  f'mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver=SQL+Server'

  engine = sa.create_engine(connectionString)

  a_date, b_date = get_date_time(dt.datetime.now())
 
  SQL_QUERY = f"""
  SELECT A.[INVOICE_NO], B.[INVOICE_DATE], B.[SHIP_TO_CUSTOMER_CODE], B.[SHIP_TO_FULL_NAME], A.[PRODUCT_CODE], A.[ATTRIBUTE21] AS PRODUCT_NAME, A.[ORDER_QTY], A.[INVOICE_AMOUNT], SUBSTRING(A.[ATTRIBUTE17], 1, 2) AS ORIGIN_COUNTRY, A.[ATTRIBUTE16] AS GTD
    FROM [1CGERP].[dbo].[XXARF_INVOICE_LINES] AS A
    INNER JOIN (
      SELECT *
          FROM [1CGERP].[dbo].[XXARF_INVOICE_HEADERS]
          WHERE ([BILL_TO_CUSTOMER_CODE] LIKE 'BY%' OR [BILL_TO_CUSTOMER_CODE] LIKE 'KZ%') AND
                  [ACCOUNTING_UNIT_CODE] = 'SVC' AND
                  [INVOICE_DATE] BETWEEN '{a_date}' AND '{b_date}'
      ) AS B ON A.[INVOICE_HEADER_ID] = B.[INVOICE_HEADER_ID]
"""

  out_file_name = dt.datetime.now().strftime('%Y%m%d') + '_SVC_out.csv'

  df = pd.read_sql_query(SQL_QUERY, con=engine)

  df.to_csv(out_file_name, index=False)

  send_email('helpdesk.ra@lge.com', sys.argv, out_file_name, out_file_name)
  print('Email sent')
else :
  print('No recipient list in command line. Please add them with comma delimeter.')

sys.exit()
