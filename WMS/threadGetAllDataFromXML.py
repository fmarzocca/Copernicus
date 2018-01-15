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
from datetime import datetime

path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0,path+'/db/')
import dbaseconfig as cfg

LOGFORMAT = '%(asctime)s - %(message)s'
LOGFILE = path+"/log/" + 'grab_X_Copernicus.log'
logging.basicConfig(filename=LOGFILE, format=LOGFORMAT, level=logging.WARN)

path = os.path.dirname(os.path.abspath(__file__))

def readData():
	try:
		db = pymysql.connect(cfg.mysql['host'],cfg.mysql['user'],cfg.mysql['passwd'],cfg.mysql['db'])
	except:
		print("Error: unable to connect to DB")
		return
	cursor = db.cursor()
	sql = "SELECT * FROM spots WHERE 1"
	try:
		cursor.execute(sql)
		results = cursor.fetchall()
	except:
		print ("Error: unable to fetch data from Database")
		db.close()
		return
	db.close()
		# id = line[5]
		# Lat = line[4]
		# Lon = line[3]
	return results


def saveSpot(lat,lon,id):
	path = os.path.dirname(os.path.abspath(__file__))
	output = subprocess.getoutput(path+"/grab_X_Copernicus.py"+ " "+lat+" "+lon)
	if not output:
		sleep(5)
		output = subprocess.getoutput(path+"/grab_X_Copernicus.py"+ " "+lat+" "+lon)
		if not output:
			return  #do not overwrite file if you can't get the data
	with open(path+"/CMEMS-NOAA/"+str(id)+".json", 'w') as f:
		f.write(output)

def getLastDate():
	url = "http://cmems-med-mfc.eu/thredds/wms/sv03-med-hcmr-wav-an-fc-h?service=WMS&version=1.3.0&request=GetCapabilities"
	BaseTag = "{http://www.opengis.net/wms}"
	try:
		root = ET.parse(urlopen(url, timeout=20)).getroot()
	except:
		logging.warning("Can't get Capabilities for last date")
		return

	for elem in root.iter(BaseTag + 'Dimension'):
		a = elem.text
		table = a.split(",")
		lastdateZ = (table[len(table) - 1])
		break
	f = open(path+'/CMEMS-lastdate.txt', 'w', encoding='utf-8')
	f.write(lastdateZ)
	f.close()

###############################

if __name__ == '__main__':
	getLastDate()
	dbData = readData()
	if not dbData:
		sys.exit()

	for line in dbData:
		saveSpot(line[4],line[3],line[5])

	# write update date/time
	now = datetime.now()
	now = now.strftime("%Y-%m-%d %H:%M")
	f = open(path+'/CMEMS-update-spots.txt', 'w', encoding='utf-8')
	f.write(now)
	f.close()


