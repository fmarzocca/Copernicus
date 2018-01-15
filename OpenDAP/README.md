# Download Data from the OpeNDAP service

These scripts access Copernicus' [OpeNDAP](http://cmems-med-mfc.eu/thredds/dodsC/sv03-med-hcmr-wav-an-fc-h.html) service to get the spots data.

## Requirements
A file stored in ./db/dbaseconfig.py contains the spots database reference.

## Usage
```
./threadGetAllDataFromOpenDAP.py
```
This will start parsing of the OpeNDAP service, by launching threads over each spot coordinate and save the result in separate json files. The script grab_Copernicus.py provides data extraction for a single spot and gathering the wind from NOAA.


