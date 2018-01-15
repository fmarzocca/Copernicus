# Download Data from the WMS service

These scripts access Copernicus' [WMS](http://cmems-med-mfc.eu/thredds/wms/sv03-med-hcmr-wav-an-fc-h?REQUEST=GetCapabilities&service=WMS) service to get the spots data.


## Requirements
A file stored in ./db/dbaseconfig.py contains the spots database reference.

## Usage
```
./threadGetAllDataFromXML.py
```
This will start parsing XML of the WMS service, by the second script over each spot coordinate and save the result in separate json files. The script grab_X_Copernicus.py provides data extraction for a single spot and gathering the wind from NOAA.