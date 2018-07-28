#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Python script to get XML forecasts from CMEMS and wind data from NOAA-GFS
# 
#  (C) Copyright 2017-2018 - Fabio Marzocca - marzoccafabio@gmail.com
# 
#  License: GPL

from urllib.request import urlopen, urlretrieve, Request
from datetime import timedelta, datetime
from threading import Thread, Lock
import sys
from urllib.error import URLError, HTTPError
import socket
import logging
import os
import json
import math
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

FROMEMAL = "......."
TOEMAIL = "......."
LOGFORMAT = '%(asctime)s - %(message)s'
LOGFILE = os.path.dirname(os.path.abspath(__file__)) + \
    "/log/" + 'grab_X_Copernicus.log'
logging.basicConfig(filename=LOGFILE, format=LOGFORMAT, level=logging.WARN)


def get_data(lat, lon):
    totalsamples = getTotalSamples()
    if not totalsamples:
        totalsamples = 33


    delta = timedelta(hours=3)
    # coords: lat,lon,lat+epsilon,lon+epsilon (string)
    epsilon = 0.01
    latepsilon = str(float(lat) + epsilon)
    lonepsilon = str(float(lon) + epsilon)
    coords = lat + "," + lon + "," + latepsilon + "," + lonepsilon
    services = ("VHM0", "VTM10", "VMDR")
    urls = []
    for service in services:
        today = datetime.now().replace(hour=00, minute=00, second=00, microsecond=0)
        timestr = today.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        for i in range(0, totalsamples):  # per ogni orario +3
            urls.append('http://nrt.cmems-du.eu/thredds/wms/sv04-med-hcmr-wav-an-fc-h?service=WMS' +
                        '&version=1.3.0&request=GetFeatureInfo&crs=EPSG:4326&bbox=' +
                        coords +
                        '&width=640&height=480&query_layers=' +
                        service + '&info_format=text/xml&i=0&j=479&time=' + timestr)
            today = today + delta
            timestr = today.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    results = [[0] * totalsamples for _ in range(4)]  # initializza array 4xtotalSamples


    threads = []
    for kk in range(len(urls)):
        # We start one thread per url present.
        process = Thread(target=crawl, args=[
                         urls[kk], results, services, kk % totalsamples])
        process.start()
        threads.append(process)

        # We now pause execution on the main thread by 'joining' all of our started threads.
        # This ensures that each has finished processing the urls.
    for process in threads:
        process.join()

    temptimeTable = results[0]
    altezzaOnde = results[1]
    periodoOnde = results[2]
    direzioneOnde = results[3]

    # timeTable[0][x] = date timeTable[1][x] = hour
    timeTable = splitDateHours(temptimeTable)
    del results

    intensitaVento, direzioneVento = getWindData(timeTable, lat, lon)
    if (intensitaVento == False) :
        intensitaVento = ['n/a'] * len(timeTable[0])
    if (direzioneVento == False) :
        direzioneVento = ['n/a'] * len(timeTable[0])

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

    return


def crawl(url, results, services, index):
    options = {'VHM0': 1, 'VTM10': 2, 'VMDR': 3, }

    try:
        root = ET.parse(urlopen(url, timeout=20)).getroot()
    except (HTTPError, URLError) as e:
        results[1][index] = "n/a"
        results[2][index] = "n/a"
        results[3][index] = "n/a"
        logging.warning("Can't get wave data: " + url +" -- "+e.reason)
        send_notice_mail("Can't get wave data: " + url +" -- "+e.reason)
        return

    featInfo = root[6]

    # find which service is in url
    service = next(x for x in services if x in url)

    timeValue = featInfo[0].text
    dataValue = featInfo[1].text
    if not dataValue == "none":
        if service == 'VMDR':
            dataValue = str("{:.0f}".format(float(dataValue)))
        else:
            dataValue = str("{:.2f}".format(float(dataValue)))
    else:
        dataValue = "n/a"
    # fill the right results column based on service in url
    results[0][index] = timeValue
    results[options[service]][index] = dataValue


def splitDateHours(data):
    temp = [[0] * len(data) for _ in range(2)]
    i = 0
    for line in data:
        t = line.find('T')
        date = line[:t]
        hour = line[t + 1:t + 3]
        temp[0][i] = date
        temp[1][i] = hour
        i += 1

    return temp


def getWindData(timeTable, spotLat, spotLon):
    BASEWINDURL = 'http://oos.soest.hawaii.edu/erddap/griddap/NCEP_Global_Best.json?ugrd10m'
    #BASEWINDURL ='http://coastwatch.pfeg.noaa.gov/erddap/griddap/NCEP_Global_Best.json?ugrd10m'
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
        return False, False
    except socket.timeout as e:
        logging.warning("Can't get wind Data: " + url + " TIMEOUT")
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
            direz = direz - 360
        #direz= (math.atan2(-compu, -compv)*180/math.pi)%360
        direz = "{:.0f}".format(direz)      # limit to 0 decimals
        direzioneVento.append(str(direz))

    return (intensitaVento, direzioneVento)

def getTotalSamples():
    try:
        lastDatefile = open("./CMEMS-lastdate.txt","r")
    except IOError as e:
        errno, strerror = e.args
        logging.warning("Can't get lastdate: I/O error({0}): {1}".format(errno,strerror) )
        send_notice_mail("Can't get lastdate: I/O error({0}): {1}".format(errno,strerror) )
        return False
    lastDate = lastDatefile.readline()
    lastDatefile.close
    try:
        lastDateDate = datetime.strptime(lastDate,'%Y-%m-%dT%H:%M:%S.%fZ')
    except (ValueError, TypeError):
        loggin.warning("Last date type/value error! This is what I read from file: ",lastDate)
        send_notice_mail("Last date type/value error! This is what I read from file: ",lastDate)
        return False
    today = datetime.now().replace(hour=00, minute=00, second=00, microsecond=0)
    diff = (lastDateDate-today).total_seconds()
    # how many 3-hours diff
    samples = int(diff/10800) 
    return samples


def exportJSON(Data):
    print(json.dumps(Data, separators=(',', ':')))

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

###############################################

if __name__ == '__main__':
    #lat = '37.08567'
    #lon = '-8.64070'
    if len(sys.argv) <= 1:
        print("Error. No coords supplied")
        sys.exit()    
    else:
        spotLat = sys.argv[1]
        spotLon = sys.argv[2]
        get_data(spotLat,spotLon)
    sys.exit()






