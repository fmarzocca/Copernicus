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
# ---- USAGE ---
# ./grab_oneSpotFromMOTU.py <lat> <lon> <path-to-NC-file>
#
#


import netCDF4
from netCDF4 import Dataset, num2date
import numpy as np
from datetime import datetime
import sys
import json
import logging
import os
from urllib.request import urlopen, urlretrieve, Request
from urllib.error import URLError, HTTPError
import math
import socket
from time import sleep

path = os.path.dirname(os.path.abspath(__file__))
LOGFORMAT = '%(asctime)s - %(message)s'
LOGFILE = path + "/log/" + 'grab_fromMOTU.log'
logging.basicConfig(filename=LOGFILE, format=LOGFORMAT, level=logging.WARN)

FROMEMAIL = "<your from address>"
TOEMAIL = "<you-to-address"

def exportJSON(Data):
    print(json.dumps(Data, separators=(',', ':')))


def getForecastData(Lat, Lon):
    global vhm0, vmdr, vtm10, lon_array, lat_array, timeTable
    x = np.abs(lon_array - float(Lon)).argmin()
    y = np.abs(lat_array - float(Lat)).argmin()

    # Sample data each 3 hours
    waveHeight = fillArray(vhm0[0::3, y, x], 2)
    wavePeriod = fillArray(vtm10[0::3, y, x], 2)
    waveDir = fillArray(vmdr[0::3, y, x], 0)

    intensitaVento, direzioneVento = getWindData(timeTable, Lat, Lon)

    if 'n/a' in intensitaVento or 'n/a' in direzioneVento:
        # Retry on NOAA error
        sleep(4)
        intensitaVento, direzioneVento = getWindData(timeTable, Lat, Lon)
        if 'n/a' in intensitaVento or 'n/a' in direzioneVento:
            #giving up and keep n/a
            pass
        else:
            logging.warning("Retrying OK wind on: " + Lat + " " + Lon)

    Data = []
    Data.append({'time': timeTable, 'waveHeight': waveHeight,
                 'wavePeriod': wavePeriod, 'waveDir': waveDir, 
                 'windSpeed': intensitaVento, 'windDir': direzioneVento})
    Data = Data[0]
    exportJSON(Data)
    del Data


def fillArray(data, decimals):
    temp = []
    for element in data:
        if element == 1e20:
            element = 'n/a'
        else:
            try:
                element = "{:.{x}f}".format(element, x=decimals)
            except:
                element = "n/a"
        element = str(element)
        temp.append(element)
    return temp


def fillTimeTable(dates):
    temp = []
    temp1 = []
    for d in dates:
        a = d.strftime('%Y-%m-%d %H')
        a = a.split(' ')
        temp.append(a[0])
        temp1.append(a[1])
    temp = np.asarray(temp)
    temp1 = np.asarray(temp1)
    temp3 = np.vstack((temp, temp1))
    del temp
    del temp1
    return temp3.tolist()


def getWindData(timeTable, spotLat, spotLon):
    failed = False
    # BASEWINDURL =
    # 'http://coastwatch.pfeg.noaa.gov/erddap/griddap/NCEP_Global_Best.json?ugrd10m'
    BASEWINDURL = 'http://oos.soest.hawaii.edu/erddap/griddap/NCEP_Global_Best.json?ugrd10m'
    intensitaVento = []
    direzioneVento = []
    startdate = timeTable[0][0] + "T" + timeTable[1][0] + ":00:00Z"
    numElements = (len(timeTable[0]))
    enddate = timeTable[0][numElements - 1] + "T" + \
        timeTable[1][numElements - 1] + ":00:00Z"
    if float(spotLon) < 0:
        spotLon = str(360 + float(spotLon))
        if float(spotLon) > 359.5:
            spotLon = str(359.49)
    reqStr = "[(" + startdate + "):1:(" + enddate + ")][(" + spotLat + "):1:(" + \
        spotLat + ")][(" + spotLon + "):1:(" + spotLon + \
        ")],vgrd10m[(" + startdate + "):1:(" + enddate + ")][(" + spotLat + \
        "):1:(" + spotLat + ")][(" + spotLon + "):1:(" + spotLon + ")]"
    url = BASEWINDURL + reqStr

    try:
        response = urlopen(url, timeout=20)
    except (HTTPError, URLError) as e:
        logging.warning("Can't get wind Data: " + url + " " + str(e.reason))
        send_notice_mail("Can't get wind Data: " + url + " " + str(e.reason))
        failed = True
    except socket.timeout as e:
        logging.warning("Can't get wind Data: " + url + " TIMEOUT")
        send_notice_mail("Can't get wind Data: " + url + " TIMEOUT")
        failed = True

    if failed == False:
        data = json.loads(response.read().decode('utf-8'))
        rows = data['table']['rows']
        ugrd = 3
        vgrd = 4

    for k in range(0, numElements):
        if failed:
            intensitaVento.append('n/a')
            direzioneVento.append('n/a')
            continue
        compu = float(rows[k][ugrd])
        compv = float(rows[k][vgrd])
        # compute speed from vectors
        vel = math.sqrt((compv * compv) + (compu * compu))
        vel = vel * 1.9438444924574  # convert to mph (knots)
        vel = "{:.2f}".format(vel)      # limit to 2 decimals
        intensitaVento.append(str(vel))
        # compute direction from vectors
        direz = 270 - (math.atan2(compv, compu) * (180 / math.pi))
        if direz > 360:
            direz = direz - 360
        # direz= (math.atan2(-compu, -compv)*180/math.pi)%360
        direz = "{:.0f}".format(direz)      # limit to 0 decimals
        direzioneVento.append(str(direz))

    return (intensitaVento, direzioneVento)

def prepareDataFromNC(ncFile):
    global vhm0, vmdr, vtm10, lon_array, lat_array, timeTable
    try:
        dataset = Dataset(ncFile)
    except:
        logging.warning("Can't assign dataset from " +ncFile)
        send_notice_mail("Can't assign dataset from " +ncFile)
        sys.exit()

    vhm0 = dataset.variables['VHM0']
    vmdr = dataset.variables['VMDR']
    vtm10 = dataset.variables['VTM10']

    time_array = dataset.variables['time'][0::3]
    t_units = dataset.variables['time'].units
    t_cal = dataset.variables['time'].calendar
    dates = num2date(time_array, units=t_units, calendar=t_cal)
    timeTable = fillTimeTable(dates)

    lon_array = dataset.variables['longitude'][:]
    lat_array = dataset.variables['latitude'][:]
    del dataset
    del time_array
    del dates

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
    #Lon = '14.4369'
    #Lat = '42.3389'
    #ncFile = "./output_full.nc"

    if len(sys.argv) <= 2:
        print("Error. No coords supplied")
        sys.exit()   

    Lat = sys.argv[1]
    Lon = sys.argv[2]
    ncFile = sys.argv[3]

    prepareDataFromNC(ncFile);
    getForecastData(Lat,Lon)

    # clear
    del vhm0
    del vtm10
    del vmdr
    del lon_array
    del lat_array
