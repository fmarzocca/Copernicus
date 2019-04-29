#!/usr/bin/env python3
#
# -*- coding: utf-8 -*-
#
# Python script to get CMEMS data from motu client
#      and wind NOAA data from ERDDAP
#
#  (C) Copyright 2018-2019 - Fabio Marzocca - marzoccafabio@gmail.com
#
#  License: GPL
#
# ---- IMPORTANT ---
# MOTU client configuration file (and MOTU code) is expected to be found in:
# $HOME/motu-client/motu-client-python.ini
#
#
# v.1.0.0 - october 2018
# v.2.0.0 - april 2019 

import pymysql
from pymysql import MySQLError
import subprocess
import sys
import os
from datetime import datetime, timedelta
import logging
import urllib.request
from urllib.error import URLError, HTTPError
import shutil
from time import strftime
import xarray as xr
import pandas as pd
import numpy as np
import tempfile
import math
import json


path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, path + '/db/')
import dbaseconfig as cfg

LOGFORMAT = '%(asctime)s - %(message)s'
LOGFILE = path + "/log/" + 'getSpotsWindWaves.log'
logging.basicConfig(filename=LOGFILE, format=LOGFORMAT, level=logging.WARN)

MOTUCLIENT = '$HOME/motu-client/motu-client.py'
OUTDIR = "/tmp/"
OUTNOAAFILE = ''
FORECAST_FILEPATH = path + '/CMEMS-NOAA/'
FROMEMAIL = "Root <fm@fabiomarzocca.com>"
TOEMAIL = "marzoccafabio@gmail.com"
endDate=""
windValid = True
NC_FILE = "/tmp/msCMEMSdaily.nc"


def getNCFiles(minLat, minLon, maxLat, maxLon):
    global endDate
    startDate = datetime.utcnow().strftime("%Y-%m-%d")
    endNOAAdate = getNOAAlastDate()
    if endNOAAdate==False:
        endNOAAdate = (datetime.now().date()+timedelta(days=6)).isoformat() + "T12:00:00Z"

    # processing: send request to MOTU to get the file url
    logging.warning("Start processing MOTU request")
    requestUrl = subprocess.getoutput(MOTUCLIENT +' -s MEDSEA_ANALYSIS_FORECAST_WAV_006_017-TDS  -x ' + minLon + ' -X ' +
                                      maxLon + ' -y ' + minLat + ' -Y ' + maxLat + ' -t ' +
                                      startDate + ' -T ' + endDate + ' -v VHM0 -v VMDR -v VTM10 -q -o console')

    if "http://" not in requestUrl:
        logging.warning('Processing MOTU request failed!')
        send_notice_mail('Processing MOTU request failed!\n' + requestUrl)
        return False

    logging.warning("Successfully processed MOTU request")

    #delete old nc file
    if (os.path.isfile(NC_FILE)):
        os.remove(NC_FILE)

    # downloading CMEMS NC file
    myurl = "http://" + requestUrl.split("http://")[1]
    try:
        logging.warning("Start downloading CMEMS NC file")
        with urllib.request.urlopen(myurl) as response, open(NC_FILE, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
    except (HTTPError, URLError) as e:
        logging.warning("Can't download CMEMS NC file: " +
                        myurl + " -> " + str(e.reason))
        send_notice_mail("Can't download CMEMS NC file: " +
                         myurl + " -> " + str(e.reason))
        return False

    logging.warning("CMEMS NC File: " + NC_FILE +
                    " successfully dowloaded.")

    # Download NOAA wind NC file
    NOAAurl = "http://oos.soest.hawaii.edu/erddap/griddap/NCEP_Global_Best.nc?ugrd10m[(" + startDate + "T00:00:00Z):1:(" + endNOAAdate + ")][(" + minLat + \
        "):1:(" + maxLat + ")][(0):1:(359.5)],vgrd10m[(" + startDate + "T00:00:00Z):1:(" + \
              endNOAAdate + ")][(" + minLat + \
        "):1:(" + maxLat + ")][(0):1:(359.5)]"

    try:
        logging.warning("Start downloading NOAA NC file")
        with urllib.request.urlopen(NOAAurl) as response, open(OUTDIR + OUTNOAAFILE, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
            logging.warning("NOAA NC File: " + OUTDIR + OUTNOAAFILE +
                    " successfully dowloaded.")
    except (HTTPError, URLError) as e:
        logging.warning("Can't download NOAA NC file: " +
                        NOAAurl + " -> " + str(e.reason))
        send_notice_mail("Can't download NOAA NC file: " +
                         NOAAurl + " -> " + str(e.reason))

    return True

def getNOAAlastDate():
    url = 'http://oos.soest.hawaii.edu/erddap/griddap/NCEP_Global_Best.json?time[last]'
    try:
        res=urllib.request.urlopen(url)
    except (HTTPError, URLError) as e:
        logging.warning("Can't get NOAA lastTime: " + str(e.reason))
        send_notice_mail("Can't get NOAA lastTime: " +  str(e.reason))
        return False
    data = json.loads(res.read().decode('utf-8'))
    lastTime = data['table']['rows'][0][0]
    return lastTime

def initDataArrays():
    global myNOAAdata, myCMEMSdata, windValid, timeTable

    # NOAA
    try:
        myNOAAdata = xr.open_dataset(OUTDIR+OUTNOAAFILE)
    except (OSError, IOError, RuntimeError) as e:
        logging.warning("Can't initialize NOAA dataset - " + " " + str(e))
        send_notice_mail("Can't initialize NOAA dataset - " + " " + str(e))
        windValid=False

    #Copernicus
    try:
        myCMEMSdata = xr.open_dataset(NC_FILE).resample(time='3H').reduce(np.mean)
        
    except (OSError, IOError, RuntimeError) as e:
        logging.warning("Can't initialize CMEMS dataset - " + " " + str(e))
        send_notice_mail("Can't initialize CMEMS dataset - " + " " + str(e))
        sys.exit()

    # time array
    dt = pd.to_datetime(myCMEMSdata.time.values)
    a0 = [str(i.date()) for i in dt]
    a1 = [str(i.time())[0:2] for i in dt]
    timeTable = np.vstack((a0, a1))
    timeTable = timeTable.tolist()

def getWavesData(spotLat, spotLon):
    x = np.abs(myCMEMSdata.longitude.values - spotLon).argmin()
    y = np.abs(myCMEMSdata.latitude.values - spotLat).argmin()
    waveH = myCMEMSdata.VHM0.values[0::1,y,x]
    waveH = waveH.tolist()
    waveHeight =[]
    for item in waveH:
        waveHeight.append("%.2f"%item)
    del waveH

    waveD = myCMEMSdata.VMDR.values[0::1,y,x]
    waveD = waveD.tolist()
    waveDirection =[]
    for item in waveD:
        waveDirection.append(int(item))
    del waveD

    waveP = myCMEMSdata.VTM10.values[0::1,y,x]
    waveP = waveP.tolist()
    wavePeriod =[]
    for item in waveP:
        wavePeriod.append("%.2f"%item)
    del waveP

    return (waveHeight, waveDirection, wavePeriod)

def getWindData(spotLat, spotLon):
    if (windValid == False):
        return (np.full((1, 36), "n/a").tolist()[0], np.full((1, 36), "n/a").tolist()[0])

    if spotLon < 0:
        spotLon = 360 + spotLon
        if spotLon > 359.5:
            spotLon = 359.49

    y = np.abs(myNOAAdata.latitude.values - spotLat).argmin()
    x = np.abs(myNOAAdata.longitude.values - spotLon).argmin()

    ugrd = myNOAAdata.ugrd10m.values[0::1, y, x]
    vgrd = myNOAAdata.vgrd10m.values[0::1, y, x]

    # wind intensity
    # build an array from 2 arrays applying for each element: sqrt(vgrd^2+ugrd^2)
    vel = np.sqrt(np.add(np.multiply(vgrd, vgrd), np.multiply(ugrd, ugrd)))
    vel = vel * 1.9438444924574  # convert to knots
    intensitaVento = []
    for item in vel:
        intensitaVento.append("%.2f" % item)

    # wind direction
    direz = 270 - np.arctan2(vgrd, ugrd) * (180 / math.pi)
    # for every array item, if direz>360, then direz=direz-360
    direz[direz > 360] = direz[direz > 360] - 360  
    direz = direz.tolist()
    direzioneVento = []
    for item in direz:
        direzioneVento.append("%.2f" % item)
    del direz
    del vel
    return(intensitaVento, direzioneVento)

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
    intensitaVento, direzioneVento = getWindData(lat, lon)
    waveHeight, waveDir, wavePeriod = getWavesData(lat,lon)

    Data = []
    Data.append({'time': timeTable, 'waveHeight': waveHeight,
                 'wavePeriod': wavePeriod, 'waveDir': waveDir, 
                 'windSpeed': intensitaVento, 'windDir': direzioneVento})
    Data = Data[0]
    print(json.dumps(Data, separators=(',', ':')), file=open(FORECAST_FILEPATH + str(id) + ".json", 'w'))

    del Data


def updateDBDate():
    try:
        db = pymysql.connect(cfg.mysql['host'], cfg.mysql[
                             'user'], cfg.mysql['passwd'], cfg.mysql['db'])
    except MySQLError as e:
        logging.warning("Error: unable to connect to DB ")
        send_notice_mail("Error: unable to connect to DB ")
        return
    cursor = db.cursor()
    sql = "UPDATE service SET updatedOn = CURRENT_DATE"
    try:
        cursor.execute(sql)
        db.commit()
    except:
        logging.warning("Error: unable to update data in Database")
        send_notice_mail("Error: unable to update data in Database")
    db.close()


def isDbUpdated():
    try:
        db = pymysql.connect(cfg.mysql['host'], cfg.mysql[
                             'user'], cfg.mysql['passwd'], cfg.mysql['db'])
    except MySQLError as e:
        logging.warning("Error: unable to connect to DB ")
        send_notice_mail("Error: unable to connect to DB ")
        return
    cursor = db.cursor()
    sql = "SELECT updatedOn FROM service"
    try:
        cursor.execute(sql)
        dateDB = cursor.fetchone()[0]
    except:
        logging.warning("Error: unable to fetch updatedOn from Database")
        send_notice_mail("Error: unable to fetch updatedOn from Database")
        db.close()
        return False
    now = datetime.now().date().strftime('%Y-%m-%d')
    if now == dateDB:
        return True
    else:
        return False


def todayProductionUpdate():
    global endDate
    requestEndDateCoverage = subprocess.getoutput(
        MOTUCLIENT + ' -s MEDSEA_ANALYSIS_FORECAST_WAV_006_017-TDS  -D -q -o console | grep "timeCoverage" ')

    if 'timeCoverage msg="OK"' not in requestEndDateCoverage:
        logging.warning('Processing MOTU EndDateCoverage request failed!')
        return False

    parsedDate = requestEndDateCoverage[requestEndDateCoverage.find(
        'end=') + 4:requestEndDateCoverage.find('start=')]
    parsedDate = parsedDate.strip().replace('"', '')
    parsedDate = parsedDate[0:10]
    
    endDate = parsedDate
    dateInterval = (datetime.strptime(
        parsedDate, "%Y-%m-%d").date() - datetime.now().date()).days

    if dateInterval >= 4:
        return True
    else:
        logging.warning('Coverage date interval <4 days!')
        return False


def send_notice_mail(text):
    from email.mime.text import MIMEText

    FROM = FROMEMAIL
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

    OUTNOAAFILE = next(tempfile._get_candidate_names()) + ".nc"

    # Check if we have already updated for today,
    # if not, check if a Product's update is already available
    if isDbUpdated() == True:
        sys.exit()
    logging.warning("Spots DB needs an update!")
    if todayProductionUpdate() == False:
        sys.exit()
    logging.warning(
        "Checked Product update available. Proceeding with update.")

    # get main CMEMS NC file
    if getNCFiles(minLat, minLon, maxLat, maxLon) == False:
        sys.exit()

    # read all spots coords from DB
    dbData = readData()
    if not dbData:
        sys.exit()

    #Initialize Datasets
    initDataArrays()

    # Analyze and save spots
    for line in dbData:
        saveSpot(float(line[4]), float(line[3]), line[5])

    # update DB updatedOn
    updateDBDate()

    # write update date/time
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M")
    g = open(path + '/CMEMS-update-spots.txt', 'w', encoding='utf-8')
    g.write(now)
    g.close()

    # delete NOAA nc file
    if (os.path.isfile(OUTDIR+OUTNOAAFILE)):
        os.remove(OUTDIR+OUTNOAAFILE)
