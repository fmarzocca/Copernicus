#!/usr/bin/env python3
#
# -*- coding: utf-8 -*-
#
# Python script to get CMEMS data from OpeNDAP server
#      and wind NOAA data from ERDDAP
#
#  (C) Copyright 2017-2018 - Fabio Marzocca - marzoccafabio@gmail.com
#
#  License: GPL
#
# ---- IMPORTANT ---
# as this script pings the MOTU server for logging purposes,
# MOTU client configuration file (and MOTU code) is expected to be found in:
# $HOME/motu-client/motu-client-python.ini
#
#

import pymysql
from pymysql import MySQLError
import subprocess
import sys
import re
import os
from threading import Thread, Lock
from time import sleep
from datetime import datetime, timedelta
import logging
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
from urllib.request import urlopen, urlretrieve, Request

path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0,path+'/db/')
import dbaseconfig as cfg

LOGFORMAT = '%(asctime)s - %(message)s'
LOGFILE = path + "/log/" + 'grab_Copernicus.log'
logging.basicConfig(filename=LOGFILE, format=LOGFORMAT, level=logging.WARN)
MOTUCLIENT = '$HOME/motu-client/motu-client.py'



def readData():
	try:
		db = pymysql.connect(cfg.mysql['host'],cfg.mysql['user'],cfg.mysql['passwd'],cfg.mysql['db'])
	except MySQLError as e:
		print("Error: unable to connect to DB ")
		return
	cursor = db.cursor()
	sql = "SELECT * FROM spots WHERE 1"
	try:
		cursor.execute(sql)
		results = cursor.fetchall()
	except:
		print("Error: unable to fetch data from Database")
		db.close()
		return
	db.close()
		# id = line[5]
		# Lat = line[4]
		# Lon = line[3]

	return results


def saveSpot(lat, lon, id):
	output = subprocess.getoutput(
	    path + "/grab_Copernicus.py" + " " + lat + " " + lon)
	if not output:
		sleep(3)
		output = subprocess.getoutput(
		    path + "/grab_Copernicus.py" + " " + lat + " " + lon)
		if not output:
			return  # do not overwrite file if you can't get the data
		else:
			logging.warning("Retrying OK on: " + lat + " " + lon + " id=" + str(id))
	with open(path + "/CMEMS-NOAA/" + str(id) + ".json", 'w') as f:
		f.write(output)


def pingMOTUserver():
    minLon = '-10'
    maxLon = "36.5"
    minLat = '30'
    maxLat = "46"
    startDate = datetime.utcnow().strftime("%Y-%m-%d")
    endDate = (datetime.utcnow() + timedelta(days=365)).strftime("%Y-%m-%d")

    # processing: send request to MOTU to get the file url
    logging.warning("Start processing MOTU request")    
    requestUrl = subprocess.getoutput(MOTUCLIENT + ' -s MEDSEA_ANALYSIS_FORECAST_WAV_006_011-TDS  -x ' + minLon + ' -X ' +
                                  maxLon + ' -y ' + minLat + ' -Y ' + maxLat + ' -t ' +
                                  startDate + ' -T ' + endDate + ' -v VHM0 -v VMDR -v VTM10 -q -o console')
    if requestUrl.startswith('http://') == False:
        logging.warning ('Processing MOTU request failed!')
        return False

    logging.warning("Successfully processed MOTU request")

###################################
if __name__ == '__main__':
	
	#ping MOTU server for logging purposes
	t = Thread(target=pingMOTUserver)
	t.start()

	dbData = readData()
	if not dbData:
		sys.exit()

	i=0
	threads=[]
	for line in dbData:
		# saveSpot(line[4],line[3],line[5])
		process = Thread(target=saveSpot, args=[line[4],line[3],line[5]])
		process.start()
		threads.append(process)
		i += 1
		if i>25:
			for process in threads:
				process.join()
			i = 0
	# write update date/time
	now = datetime.now()
	now = now.strftime("%Y-%m-%d %H:%M")
	g = open(path+'/CMEMS-update-spots.txt', 'w', encoding='utf-8')
	g.write(now)
	g.close()

	

