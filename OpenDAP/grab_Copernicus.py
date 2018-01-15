#!/usr/local/bin/python3


# -*- coding: utf-8 -*-
#
# Python script to get OpeNDap forecasts from CMEMS and wind data from NOAA-GFS
# 
#  (C) Copyright 2017 - Fabio Marzocca - marzoccafabio@gmail.com
# 
#  License: GPL

# get the structure

from urllib.request import urlopen, urlretrieve, Request
from urllib.error import URLError, HTTPError
import time
import numpy as np
import re
import logging
import os
import sys
from datetime import datetime
import json
import math
import socket


LOGFORMAT = '%(asctime)s - %(message)s'
LOGFILE = os.path.dirname(os.path.abspath(__file__)) + \
    "/log/" + 'grab_Copernicus.log'
logging.basicConfig(filename=LOGFILE, format=LOGFORMAT, level=logging.WARN)
BASEURL = 'http://cmems-med-mfc.eu/thredds/dodsC/sv03-med-hcmr-wav-an-fc-h'


def getDataStructure():
    dimensions = {}
    url = BASEURL + '.dds'
    try:
        response = urlopen(url, timeout=20)
    except (HTTPError, URLError) as e:
        logging.warning("Can't get dds data Structure: " +
                        url + " " + str(e.reason))
        send_notice_mail("Can't get dds data Structure: " +
                         url + " " + str(e.reason))
        return False
    except socket.timeout as e:
        logging.warning("Can't get dds data Structure: " +
                        url + " TIMEOUT")
        send_notice_mail("Can't get dds data Structure: " +
                         url + " TIMEOUT")
        return False

    data = response.read()      # a `bytes` object
    textdata = data.decode('utf-8')
    header = textdata.split('\n')

    for x in range(1, 4):
        if 'Int32 time[time = ' in header[x]:
            foundtime = header[x].split('Int32 time[time = ', 1)
            foundtime = (foundtime[1].split(']')[0])
            dimensions["time"] = foundtime
            continue
        if 'Float32 lat[lat = ' in header[x]:
            foundlat = header[x].split('Float32 lat[lat = ', 1)
            foundlat = (foundlat[1].split(']')[0])
            dimensions['lat'] = foundlat
            continue
        if 'Float32 lon[lon = ' in header[x]:
            foundlon = header[x].split('Float32 lon[lon = ', 1)
            foundlon = (foundlon[1].split(']')[0])
            dimensions['lon'] = foundlon
    return dimensions


def getTimeIdx(dim):
    timeDeltaCMEMS = 12*3600
    url = BASEURL + '.ascii?time[0:1:' + str((int(dim) - 1)) + ']'
    try:
        response = urlopen(url, timeout=20)
    except (HTTPError, URLError) as e:
        logging.warning("Can't get Time start index: " +
                        url + " " + str(e.reason))
        send_notice_mail("Can't get Time start index: " +
                         url + " " + str(e.reason))
        return False
    except socket.timeout as e:
        logging.warning("Can't get Time start index: " +
                        url + " TIMEOUT")
        send_notice_mail("Can't get Time start index: " +
                         url + " TIMEOUT")
        return False

    data = response.read()      # a `bytes` object
    textdata = data.decode('utf-8')
    data = textdata.split('\n')
    del data[:5]  # remove first 5 lines
    data = list(filter(None, data))
    for element in data:
        values = element.split(', ')
    del data
    today = datetime.utcnow().replace(hour=00, minute=00, second=00, microsecond=0)
    # oppure dalla riga di sopra togli hours così prende l'ora corrente
    todayCMEMSHours = int((today.timestamp() - timeDeltaCMEMS) / 3600)

    try:
        index = values.index(str(todayCMEMSHours))
    except ValueError:
        logging.warning("Can't get time vallue for today: " +
                        todayCMEMSHours + " " + str(e.reason))
        send_notice_mail("Can't get time vallue for today: " +
                         todayCMEMSHours + " " + str(e.reason))
        return False
    del values
    return (index)


def getLatLongIdx(latdim, londim, spotLat, spotLon):
    latdim = str((int(latdim) - 1))
    londim = str((int(londim) - 1))
    # Latitude list
    url = BASEURL + '.ascii?lat[0:1:' + latdim + ']'
    try:
        response = urlopen(url, timeout=20)
    except (HTTPError, URLError) as e:
        logging.warning("Can't get Lat index: " +
                        url + " " + str(e.reason))
        send_notice_mail("Can't get Lat start index: " +
                         url + " " + str(e.reason))
        return False, False
    except socket.timeout as e:
        logging.warning("Can't get Lat start index: " +
                        url +" TIMEOUT")
        send_notice_mail("Can't get Lat start index: " +
                         url + " TIMEOUT")
        return False, False

    data = response.read()      # a `bytes` object
    textdata = data.decode('utf-8')
    data = textdata.split('\n')
    del data[:5]  # remove first 5 lines
    data = list(filter(None, data))
    for element in data:
        temp = element.split(', ')
    del data
    temp = np.asarray(temp)
    latitudes = temp.astype(np.float)
    minLat = searchCoordinate(latitudes, spotLat)
    del temp
    del latitudes

    # Longitude list
    url = BASEURL + '.ascii?lon[0:1:' + londim + ']'
    try:
        response = urlopen(url, timeout=20)
    except (HTTPError, URLError) as e:
        logging.warning("Can't get Lon index: " +
                        url + " " + str(e.reason))
        send_notice_mail("Can't get Lon start index: " +
                         url + " " + str(e.reason))
        return False, False
    except socket.timeout as e:
        logging.warning("Can't get Lon start index: " +
                        url + " TIMEOUT")
        send_notice_mail("Can't get Lon start index: " +
                         url + " TIMEOUT")
        return False, False
    data = response.read()      # a `bytes` object
    textdata = data.decode('utf-8')
    data = textdata.split('\n')
    del data[:5]  # remove first 5 lines
    data = list(filter(None, data))
    for element in data:
        temp = element.split(', ')
    del data
    temp = np.asarray(temp)
    longitudes = temp.astype(np.float)
    minLon = searchCoordinate(longitudes, spotLon)
    del temp
    del longitudes
    return minLon, minLat


def searchCoordinate(coordinates, spotcoord):
    idx = (np.abs(coordinates - float(spotcoord))).argmin()
    return idx


def getWaveData(dimTime, minTime, minLat, minLon):
    dimTime = int(dimTime) - 1
    dimTime = str(dimTime)
    reqStr = "[" + str(minTime) + ":3:" + dimTime + "][" + str(minLat) + \
        ":1:" + str(minLat) + "][" + str(minLon) + ":1:" + str(minLon) + "]"
    url = BASEURL + ".ascii?VHM0" + reqStr + ",VTM10" + reqStr + ",VMDR" + reqStr

    try:
        response = urlopen(url, timeout=20)
    except (HTTPError, URLError) as e:
        logging.warning("Can't get wave Data: " + url + " " + str(e.reason))
        send_notice_mail("Can't get wave Data: " + url + " " + str(e.reason))
        return False, False, False, False
    except socket.timeout as e:
        logging.warning("Can't get wave Data: " + url + " TIMEOUT")
        send_notice_mail("Can't get wave Data: " + url + " TIMEOUT")
        return False, False, False, False
    data = response.read()      # a `bytes` object
    textdata = data.decode('utf-8')
    data = textdata.split('\n')
    del data[:27]
    numElements = re.search(r"\[([0-9_]+)\]", data[0])
    numElements = numElements.group(1)

    altezzaOnde = fillArray(numElements, data, 'VHM0', "2")
    if altezzaOnde == False:
        logging.warning("Can't fill Array of data VHM0 ")
        send_notice_mail("Can't fill Array of data VHM0 ")
        return False, False, False, False
    periodoOnde = fillArray(numElements, data, 'VTM10', "2")
    if periodoOnde == False:
        logging.warning("Can't fill Array of data VTM10: ")
        send_notice_mail("Can't fill Array of data VTM10: ")
        return False, False, False, False
    direzioneOnde = fillArray(numElements, data, 'VMDR', "0")
    if direzioneOnde == False:
        logging.warning("Can't fill Array of data VMDR ")
        send_notice_mail("Can't fill Array of data VMDR ")
        return False, False, False, False
    # timeTable[0][x] = days ---- timeTable[1][x] = hours
    timeTable = fillTimeTable(numElements, data)
    return (altezzaOnde, periodoOnde, direzioneOnde, timeTable)


def fillArray(numElements, data, variable, decimals):
    temp = []
    i = 0
    for line in data:
        if (line.find(variable + '.' + variable) != -1):
            start = i
            for k in range(0, int(numElements)):
                # prendi solo il dato dopo la virgola
                element = data[start + 1 + k].split(', ')
                element = float(element[1])  # converti in float
                if element == 1e20:
                    return False
                element = "{:.{x}f}".format(
                    element, x=decimals)  # riduci a 2 decimali
                element = str(element)
                temp.append(element)
            return temp
        i += 1
        continue
    return False


def fillTimeTable(numElements, data):
    timeDeltaCMEMS = 12*3600
    temp = []
    temp1 = []
    i = 0
    for line in data:
        if (line.find('VHM0.time') != -1):
            start = i
            element = data[i + 1].split(', ')
            for k in range(0, int(numElements)):
                a = datetime.fromtimestamp(
                    (int(element[k]) * 3600)+timeDeltaCMEMS).strftime('%Y-%m-%d %H')
                a = a.split(' ')
                temp.append(a[0])
                temp1.append(a[1])
            temp = np.asarray(temp)
            temp1 = np.asarray(temp1)
            temp3 = np.vstack((temp, temp1))
            del temp
            del temp1
            return temp3.tolist()

        i += 1
    return False


def getWindData(timeTable, spotLat, spotLon):
    #BASEWINDURL = 'http://coastwatch.pfeg.noaa.gov/erddap/griddap/NCEP_Global_Best.json?ugrd10m'
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
        return False, False
    except socket.timeout as e:
        logging.warning("Can't get wind Data: " + url + " TIMEOUT" )
        send_notice_mail("Can't get wind Data: " + url + " TIMEOUT")
        return False, False

    data = json.loads(response.read().decode('utf-8'))
    rows = data['table']['rows']
    ugrd = 3
    vgrd = 4
    for k in range(0, numElements):
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
            direz = direz-360
        #direz= (math.atan2(-compu, -compv)*180/math.pi)%360
        direz = "{:.0f}".format(direz)      # limit to 0 decimals
        direzioneVento.append(str(direz))

    return (intensitaVento, direzioneVento)

def exportJSON(Data):
    print(json.dumps(Data, separators=(',', ':')))


def send_notice_mail(text):
    from email.mime.text import MIMEText

    FROM = "Root <fm@fabiomarzocca.com>"
    TO = "marzoccafabio@gmail.com"

    SUBJECT = "MeteoSurf notice (Copernicus)!"

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


def pre_main(*argv):
    #if called from an external function
    if len(argv)==0:
        print("Error. No coords supplied")
        sys.exit()    
    else:
        spotLat = argv[0]
        spotLon = argv[1]
        main_entry(spotLat,spotLon)        


def main_entry(spotLat,spotLon):

    # get the data structure and the dimensions of the variables time, lat,
    # lon
    dimensions = getDataStructure()
    if (dimensions == False):
        #print("Error on gathering data structure")
        sys.exit()

    # get the index of the first starting date for forecast
    minTime = getTimeIdx(dimensions['time'])  # index of starting day
    if (minTime == False):
        #print("Error getting Time start index!")
        sys.exit()

    # get the coordinates for the spot in the table
    minLon, minLat = getLatLongIdx(dimensions['lat'], dimensions[
                                   'lon'], spotLat, spotLon)
    if(minLon == False or minLat == False):
        #print("Error getting LatLon indexes")
        sys.exit()

    # Get waves info from Copernicus
    altezzaOnde, periodoOnde, direzioneOnde, timeTable = getWaveData(
        dimensions['time'], minTime, minLat, minLon)
    if (altezzaOnde == False or periodoOnde == False or direzioneOnde == False):
        #print("Error on gathering main wave data")
        sys.exit()

    # Get Wind info from GFS NOAA
    intensitaVento, direzioneVento = getWindData(timeTable, spotLat, spotLon)
    if (intensitaVento == False or direzioneVento == False):
        #print("Error getting Wind data!")
        sys.exit()

    Data = []
    Data.append({'time': timeTable, 'waveHeight': altezzaOnde,
                 'wavePeriod': periodoOnde, 'waveDir': direzioneOnde, 'windSpeed': intensitaVento, 'windDir': direzioneVento})
    Data = Data[0]

    del timeTable
    del altezzaOnde
    del periodoOnde
    del direzioneOnde
    del intensitaVento
    del direzioneVento

    exportJSON(Data)
    del Data


###############################################

if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print("Error. No coords supplied")
        sys.exit()    
    else:
        spotLat = sys.argv[1]
        spotLon = sys.argv[2]
        main_entry(spotLat,spotLon)
    sys.exit()




# http://cmems-med-mfc.eu/thredds/dodsC/sv03-med-hcmr-wav-an-fc-h.ascii?VHM0[9862:1:9935][284:1:284][720:1:720],VTPK[9862:1:9935][284:1:284][720:1:720],VMDR[9862:1:9935][284:1:284][720:1:720]
