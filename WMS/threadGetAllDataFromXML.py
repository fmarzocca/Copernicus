#!/usr/bin/env python3

import pymysql
import subprocess
import sys
import re
import os
from threading import Thread, Lock
from time import sleep
import logging
from urllib.request import urlopen, urlretrieve, Request
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, path + '/db/')
import dbaseconfig as cfg

LOGFORMAT = '%(asctime)s - %(message)s'
LOGFILE = path + "/log/" + 'grab_X_Copernicus.log'
logging.basicConfig(filename=LOGFILE, format=LOGFORMAT, level=logging.WARN)

path = os.path.dirname(os.path.abspath(__file__))
MOTUCLIENT = '$HOME/motu-client/motu-client.py'
FROMEMAL = "......."
TOEMAIL = "......."


def readData():
    try:
        db = pymysql.connect(cfg.mysql['host'], cfg.mysql[
                             'user'], cfg.mysql['passwd'], cfg.mysql['db'])
    except:
        print("Error: unable to connect to DB")
        send_notice_mail("Error: unable to connect to DB")
        return False
    cursor = db.cursor()
    sql = "SELECT * FROM spots WHERE 1"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
    except:
        print("Error: unable to fetch data from Database")
        send_notice_mail("Error: unable to fetch data from Database")
        db.close()
        return False
    db.close()
    # id = line[5]
    # Lat = line[4]
    # Lon = line[3]
    return results


def saveSpot(lat, lon, id):
    path = os.path.dirname(os.path.abspath(__file__))
    output = subprocess.getoutput(
        path + "/grab_X_Copernicus.py" + " " + lat + " " + lon)
    if not output:
        sleep(5)
        output = subprocess.getoutput(
            path + "/grab_X_Copernicus.py" + " " + lat + " " + lon)
        if not output:
            return  # do not overwrite file if you can't get the data
    with open(path + "/CMEMS-NOAA/" + str(id) + ".json", 'w') as f:
        f.write(output)


def getLastDate():
    url = "http://nrt.cmems-du.eu/thredds/wms/sv04-med-hcmr-wav-an-fc-h?service=WMS&version=1.3.0&request=GetCapabilities"
    BaseTag = "{http://www.opengis.net/wms}"
    try:
        root = ET.parse(urlopen(url, timeout=20)).getroot()
    except:
        logging.warning("Can't get Capabilities for last date: "+url)
        send_notice_mail("Can't get Capabilities for last date: "+url)
        sys.exit()
        return

    for elem in root.iter(BaseTag + 'Dimension'):
        a=elem.text
        table = a.split('/')
        lastdateZ = table[len(table)-2]
        break

    f = open(path + '/CMEMS-lastdate.txt', 'w', encoding='utf-8')
    f.write(lastdateZ)
    f.close()

def pingMOTUserver():
    minLon = '-10'
    maxLon = "36.5"
    minLat = '30'
    maxLat = "46"
    startDate = datetime.utcnow().strftime("%Y-%m-%d")
    endDate = (datetime.utcnow() + timedelta(days=365)).strftime("%Y-%m-%d")

    # processing: send request to MOTU to get the file url
    logging.warning("Start processing MOTU request")
    requestUrl = subprocess.getoutput(MOTUCLIENT + ' -s MEDSEA_ANALYSIS_FORECAST_WAV_006_017-TDS  -x ' + minLon + ' -X ' +
                                      maxLon + ' -y ' + minLat + ' -Y ' + maxLat + ' -t ' +
                                      startDate + ' -T ' + endDate + ' -v VHM0 -v VMDR -v VTM10 -q -o console')
    if "http://" not in requestUrl:
        logging.warning('Processing MOTU request failed!')
        return False

    logging.warning("Successfully processed MOTU request")

def send_notice_mail(text):
    from email.mime.text import MIMEText

    FROM = FROMEMAL
    TO = TOEMAIL

    SUBJECT = "MeteoSurf notice (CMEMS from XML)!"

    TEXT = text
    SENDMAIL = "/usr/sbin/sendmail"
    # Prepare actual message
    msg = MIMEText(text)
    msg["From"] = FROM
    msg["To"] = TO
    msg["Subject"] = SUBJECT

    # Send the mail
    import os

    p = os.popen("%s -t -i" % SENDMAIL, "w")
    p.write(msg.as_string())
    status = p.close()
    if status:
        logging.warning("Sendmail exit status ", status)

###############################

if __name__ == '__main__':
        # ping MOTU server for logging purposes
    t = Thread(target=pingMOTUserver)
    t.start()

    getLastDate()
    dbData = readData()
    if not dbData:
        sys.exit()
    #i=0
    #threads=[]
    for line in dbData:
        saveSpot(line[4], line[3], line[5])
        #process = Thread(target=saveSpot, args=[line[4],line[3],line[5]])
        #process.start()
        #threads.append(process)
        #i += 1
        #if i>10:
            #for process in threads:
                #process.join()
            #i = 0

    # write update date/time
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M")
    f = open(path + '/CMEMS-update-spots.txt', 'w', encoding='utf-8')
    f.write(now)
    f.close()
