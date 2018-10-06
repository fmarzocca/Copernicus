# Download Data by using the MOTU python client

These scripts use the [python MOTU client](https://github.com/clstoulouse/motu-client-python) to download data from CMEMS.

## Requirements
The MOTU client code and the motu-client-python.ini file must be saved in $HOME/motu-client/ folder. A file stored in ./db/dbaseconfig.py contains the spots database reference.


## Usage
```
./getSpotsWindWaves.py
```
This will download the NC files from CMEMS and NOAA, accesses the database and extract data for each single spot, saving the results in separate json files.  
It uses the xarray module for python.




