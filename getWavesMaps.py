#!/usr/bin/env python3
#
# -*- coding: utf-8 -*-
#
# Python script to get maps wave height and direction from CMEMS data NetCDF format 
#
#  (C) Copyright 2019 - Fabio Marzocca - marzoccafabio@gmail.com
#
#  License: GPL
#
# ---- IMPORTANT ---
# MOTU client configuration file (and MOTU code) is expected to be found in:
# $HOME/motu-client/motu-client-python.ini
#
# v.1.0.0 - april 2019


import matplotlib
matplotlib.use('Agg')
import xarray as xr
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import logging
import tempfile
import sys
import os
from dateutil import parser
import shutil
from datetime import datetime, timedelta, date
import subprocess
import urllib.request
from urllib.error import URLError, HTTPError
from time import strftime

path = os.path.dirname(os.path.abspath(__file__))

LOGFORMAT = '%(asctime)s - %(message)s'
LOGFILE = path + "/log/" + 'getWavesMaps.log'
logging.basicConfig(filename=LOGFILE, format=LOGFORMAT, level=logging.WARN)
FROMEMAIL = "Root <fm@fabiomarzocca.com>"
TOEMAIL = "marzoccafabio@gmail.com"


TEMPDIR = "/tmp/CMEMSmaps/"
NC_FILE= "/tmp/msCMEMSdaily.nc"
MOTUCLIENT = '$HOME/motu-client/motu-client.py'


def getNCFiles(minLat, minLon, maxLat, maxLon):
    startDate = datetime.utcnow().strftime("%Y-%m-%d")
    endDate = (datetime.now().date()+timedelta(days=10)).strftime("%Y-%m-%d")

    # processing: send request to MOTU to get the file url
    logging.warning("Start processing MOTU request")
    requestUrl = subprocess.getoutput(MOTUCLIENT + ' -s MEDSEA_ANALYSIS_FORECAST_WAV_006_017-TDS  -x ' + minLon + ' -X ' +
                                      maxLon + ' -y ' + minLat + ' -Y ' + maxLat + ' -t ' +
                                      startDate + ' -T ' + endDate + ' -v VHM0 -v VMDR -q -o console')

    if "http://" not in requestUrl:
        logging.warning('Processing MOTU for maps request failed!')
        send_notice_mail('Processing MOTU for maps request failed!\n' + requestUrl)
        return False

    logging.warning("Successfully processed MOTU request")

    # downloading CMEMS NC file
    myurl = "http://" + requestUrl.split("http://")[1]
    try:
        logging.warning("Start downloading CMEMS NC file")
        with urllib.request.urlopen(myurl) as response, open(NC_FILE, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
    except (HTTPError, URLError) as e:
        logging.warning("Can't download CMEMS NC file: " +
                        myurl + " -> " + str(e.reason))
        send_notice_mail("Can't download CMEMS NC file for maps: " +
                         myurl + " -> " + str(e.reason))
        return False

    logging.warning("CMEMS NC File: " + NC_FILE +
                    " successfully dowloaded.")
    return True

def testOneShot(ncfile):
    myCMEMSdata = xr.open_dataset(ncfile).resample(time='3H').reduce(np.mean)

    # time 
    lastDate =myCMEMSdata.time.values[myCMEMSdata.time.values.size-1]
    startDate = myCMEMSdata.time.values[0]

    print(startDate," - ", lastDate)
    print(myCMEMSdata.time.values.size)

    plt.figure(figsize=(20.48, 10.24))
    plt.clf()

    # projection, lat/lon extents and resolution of polygons to draw
    # resolutions: c - crude, l - low, i - intermediate, h - high, f - full
    map = Basemap(projection='merc', llcrnrlon=-10.,
                  llcrnrlat=30., urcrnrlon=36.5, urcrnrlat=46.)
    #map.etopo()
    map.shadedrelief(scale=0.65)


    X, Y = np.meshgrid(myCMEMSdata.longitude.values,
                       myCMEMSdata.latitude.values)
    x, y = map(X, Y)

    waveH = myCMEMSdata.VHM0.values
    wDir = myCMEMSdata.VMDR.values
    del myCMEMSdata
 

    my_cmap = plt.get_cmap('rainbow')
    onde = map.pcolormesh(x, y, waveH[0,:,:], cmap=my_cmap, norm=matplotlib.colors.LogNorm(vmin=0.07, vmax=4.,clip=True))
    
    #plt.colorbar();

    # waves direction
    # reduce arrows density (1 out of 15)
    yy = np.arange(0, y.shape[0], 15)
    xx = np.arange(0, x.shape[1], 15)
    points = np.meshgrid(yy,xx)

    wDir = wDir[0,:,:]
    
    map.quiver(x[points],y[points],np.cos(np.deg2rad(wDir[points])),np.sin(np.deg2rad(wDir[points])),
    	edgecolor='lightgray', minshaft=4,  width=0.007, headwidth=3., headlength=4., linewidth=.5)

    plt.show()
    plt.savefig("prova_s065.jpg", quality=75)
    plt.close()
   

def getMaps(ncfile):
    myCMEMSdata = xr.open_dataset(ncfile).resample(time='3H').reduce(np.mean)

    plt.figure(figsize=(20.48, 10.24))

    # projection, lat/lon extents and resolution of polygons to draw
    # resolutions: c - crude, l - low, i - intermediate, h - high, f - full
    map = Basemap(projection='merc', llcrnrlon=-10.,
                  llcrnrlat=30., urcrnrlon=36.5, urcrnrlat=46.)


    X, Y = np.meshgrid(myCMEMSdata.longitude.values,
                       myCMEMSdata.latitude.values)
    x, y = map(X, Y)

    # reduce arrows density (1 out of 15)
    yy = np.arange(0, y.shape[0], 15)
    xx = np.arange(0, x.shape[1], 15)
    points = np.meshgrid(yy,xx)

    #cycle time to save maps
    i=0
    while i < myCMEMSdata.time.values.size:
        map.shadedrelief(scale=0.65)
        #waves height
        waveH = myCMEMSdata.VHM0.values[i, :, :]
        my_cmap = plt.get_cmap('rainbow')
        map.pcolormesh(x, y, waveH, cmap=my_cmap, norm=matplotlib.colors.LogNorm(vmin=0.07, vmax=4.,clip=True))
        # waves direction
        wDir = myCMEMSdata.VMDR.values[i, :, :]
        map.quiver(x[points],y[points],np.cos(np.deg2rad(wDir[points])),np.sin(np.deg2rad(wDir[points])),
            edgecolor='lightgray', minshaft=4,  width=0.007, headwidth=3., headlength=4., linewidth=.5)
        # save plot
        filename = pd.to_datetime(myCMEMSdata.time[i].values).strftime("%Y-%m-%d_%H")
        plt.show()
        plt.savefig(TEMPDIR+filename+".jpg", quality=75)
        plt.clf()
        del wDir
        del waveH
        i += 1

    #out of loop
    plt.close("all")
    del myCMEMSdata

def mapsUpdated():
    try:
        f = open(path+"/CMEMS-update-maps.txt")
    except:
        logging.warning("Error: unable read CMEMS-update-maps.txt file ")
        send_notice_mail("Error: unable read CMEMS-update-maps.txt file")
        return False
    first = f.readline()
    f.close()
    upd= datetime.strptime(first.split(' ')[0], "%Y-%m-%d")
    if upd.date() < date.today():
        return False
    return True
   
def todayProductionUpdate():
    requestEndDateCoverage = subprocess.getoutput(
        MOTUCLIENT + ' -s MEDSEA_ANALYSIS_FORECAST_WAV_006_017-TDS  -D -q -o console | grep "timeCoverage" ')

    if 'timeCoverage msg="OK"' not in requestEndDateCoverage:
        logging.warning('Processing MOTU EndDateCoverage request failed!')
        return False

    parsedDate = requestEndDateCoverage[requestEndDateCoverage.find(
        'end=') + 4:requestEndDateCoverage.find('start=')]
    parsedDate = parsedDate.strip().replace('"', '')
    parsedDate = parsedDate[0:10]

    dateInterval = (datetime.strptime(
        parsedDate, "%Y-%m-%d").date() - datetime.now().date()).days

    if dateInterval >= 4:
        return True
    else:
        logging.warning('Coverage date interval <4 days!')
        return False


def moveFiles():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    shutil.rmtree(os.getcwd() + "/CMEMSmaps", ignore_errors=True)
    shutil.move(TEMPDIR, os.getcwd())

def ncFileDownloaded():
    try:
        ct = os.path.getctime(NC_FILE)
    except:
        return False
    fileCreated=datetime.fromtimestamp(ct)
    diff = datetime.now() - fileCreated
    if diff.days > 0:
        return False
    return True


def send_notice_mail(text):
    from email.mime.text import MIMEText

    FROM = FROMEMAIL
    TO = TOEMAIL

    SUBJECT = "MeteoSurf notice (getWavesMaps)!"

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

    #testOneShot(NC_FILE)
    #sys.exit()

    # Check if we have already updated for today,
    # if not, check if a Product's update is already available
    if mapsUpdated()==True:
        sys.exit()
    logging.warning("Maps need an update!")
    if todayProductionUpdate() == False:
        sys.exit()
    logging.warning(
        "Checked Product update available. Proceeding with update.")

    # get main CMEMS NC file
    if ncFileDownloaded() == False:
        if getNCFiles(minLat, minLon, maxLat, maxLon) == False:
            sys.exit()

    # clear and restate temp dir
    shutil.rmtree(TEMPDIR, ignore_errors=True)
    try:
        os.makedirs(TEMPDIR)
    except:
        pass

    getMaps(NC_FILE)
    moveFiles()

    # write update date/time
    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M")
    g = open(path+'/CMEMS-update-maps.txt', 'w', encoding='utf-8')
    g.write(now)
    g.close()
    logging.warning("Application completed")
    sys.exit()
