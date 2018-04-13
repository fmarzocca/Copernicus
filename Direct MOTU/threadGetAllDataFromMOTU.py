#!/usr/bin/env python3
#
# -*- coding: utf-8 -*-
#
# Python script to get CMEMS data from motu client 
#      and wind NOAA data from ERDDAP
#
#  (C) Copyright 2018 - Fabio Marzocca - marzoccafabio@gmail.com
#
#  License: GPL
#
# ---- IMPORTANT ---
# MOTU client configuration file (and MOTU code) is expected to be found in:
# $HOME/motu-client/motu-client-python.ini
#
#

import pymysql
from pymysql import MySQLError
import subprocess
import sys
import os
from threading import Thread, Lock
from datetime import datetime, timedelta
import logging
import urllib.request
from urllib.error import URLError, HTTPError
import shutil


path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, path + '/db/')
import dbaseconfig as cfg

LOGFORMAT = '%(asctime)s - %(message)s'
LOGFILE = path + "/log/" + 'grab_fromMOTU.log'
logging.basicConfig(filename=LOGFILE, format=LOGFORMAT, level=logging.WARN)

MOTUCLIENT = '$HOME/motu-client/motu-client.py'
OUTDIR = "/tmp/"
OUTFILE = 'CMEMS_006_017.nc'
FORECAST_FILEPATH = path + '/CMEMS-NOAA/'
FROMEMAIL = "<your from address>"
TOEMAIL = "<you-to-address"


def getNCFile(minLat, minLon, maxLat, maxLon):
    startDate = datetime.utcnow().strftime("%Y-%m-%d")
    endDate = (datetime.utcnow() + timedelta(days=365)).strftime("%Y-%m-%d")

    # processing: send request to MOTU to get the file url
    logging.warning("Start processing MOTU request")    
    requestUrl = subprocess.getoutput(MOTUCLIENT + ' -s MEDSEA_ANALYSIS_FORECAST_WAV_006_017-TDS  -x ' + minLon + ' -X ' +
                                  maxLon + ' -y ' + minLat + ' -Y ' + maxLat + ' -t ' +
                                  startDate + ' -T ' + endDate + ' -v VHM0 -v VMDR -v VTM10 -q -o console')

    if "http://" not in requestUrl:
        logging.warning ('Processing MOTU request failed!')
        send_notice_mail('Processing MOTU request failed!\n'+requestUrl)
        return False

    logging.warning("Successfully processed MOTU request")

    #downloading NC file
    myurl = "http://"+requestUrl.split("http://")[1]
    try:
        logging.warning("Start downloading NC file") 
        with urllib.request.urlopen(myurl) as response, open(OUTDIR+OUTFILE, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
    except (HTTPError, URLError) as e:
        logging.warning("Can't download main NC file: "+myurl+" -> "+str(e.reason))
        send_notice_mail("Can't download main NC file: "+myurl+" -> "+str(e.reason))
        return False

    logging.warning("NC File: " + OUTDIR + OUTFILE +
                        " successfully dowloaded.")
    return True

def readData():
    try:
        db = pymysql.connect(cfg.mysql['host'], cfg.mysql[
                             'user'], cfg.mysql['passwd'], cfg.mysql['db'])
    except MySQLError as e:
        logging.warning("Error: unable to connect to DB ")
        send_notice_mail("Error: unable to connect to DB ")
        return
    cursor = db.cursor()
    sql = "SELECT * FROM spots WHERE 1"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
    except:
        logging.warning("Error: unable to fetch data from Database")
        send_notice_mail("Error: unable to fetch data from Database")
        db.close()
        return
    db.close()
    # id = line[5]
    # Lat = line[4]
    # Lon = line[3]
    return results


def saveSpot(lat, lon, id):
    output = subprocess.getoutput(
        path + "/grab_oneSpotFromMOTU.py" + " " + lat + " " + lon + " " + OUTDIR + OUTFILE)
    if not output:
        logging.warning("No output for spot id: " + id)
        send_notice_mail("No output for spot id: " + id)
        # do not overwrite previous forecast file
        return
    with open(FORECAST_FILEPATH + str(id) + ".json", 'w') as f:
        f.write(output)


def send_notice_mail(text):
    from email.mime.text import MIMEText

    FROM = FROMEMAL
    TO = TOEMAIL

    SUBJECT = "MeteoSurf notice (CMEMS from MOTU)!"

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


###################################
if __name__ == '__main__':

    minLon = '-10'
    maxLon = "36.5"
    minLat = '30'
    maxLat = "46"


    # get main CMEMS NC file
    if getNCFile(minLat, minLon, maxLat,maxLon) == False:
        print("Can't get main file")
        sys.exit()


    # read all spots coords from DB
    dbData = readData()
    if not dbData:
        sys.exit()

    # threading
    i = 0
    threads = []
    for line in dbData:
        # args Lat, Lon, id
        process = Thread(target=saveSpot, args=[line[4], line[3], line[5]])
        process.start()
        threads.append(process)
        i += 1
        # Limit to 25 simultaneous threads
        if i > 25:
            for process in threads:
                process.join()
            i = 0
    # write update date/time
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M")
    g = open(path + '/CMEMS-update-spots.txt', 'w', encoding='utf-8')
    g.write(now)
    g.close()
