# Download Data by using the MOTU python client

These scripts use the [python MOTU client](https://github.com/clstoulouse/motu-client-python) to download data from CMEMS.

## Requirements
The MOTU client code and the motu-client-python.ini file must be saved in $HOME/motu-client/ folder. A file stored in ./db/dbaseconfig.py contains the spots database reference.


## Usage
```
./threadGetAllDataFromMOTU.py
```
This will download the NC file from CMEMS, accesses the database and launches threads to extract data for each single spot and saves the results in separate json files. The script grab_oneSpotFromMOTU.py provides data extraction from NC file and gathering the wind from NOAA.



