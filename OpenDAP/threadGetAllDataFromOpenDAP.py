#!/usr/bin/env python3

import grab_Copernicus
import pymysql
from pymysql import MySQLError
import subprocess
import sys
import re
import os
from threading import Thread, Lock
from time import sleep
from datetime import datetime
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



###################################
if __name__ == '__main__':
	
	getLastDate()
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
		if i>50:
			for process in threads:
				process.join()
			i = 0
	# write update date/time
	now = datetime.now()
	now = now.strftime("%Y-%m-%d %H:%M")
	g = open(path+'/CMEMS-update-spots.txt', 'w', encoding='utf-8')
	g.write(now)
	g.close()
	f = open(path+'/CMEMS-update.txt', 'w', encoding='utf-8')
	f.write(now)
	f.close()
	

