# Python scripts to download data from Copernicus

These scripts are backend helpers for the application [MeteoSurf](http://www.marzocca.net/linux/meteosurf_en.html) and they achieve the same result using different methods to download spot wave data from CMEMS/Copernicus, dataset MEDSEA_ANALYSIS_FORECAST_WAV_006_017. 

The scripts use python 3.xx

## Requirements
The MOTU client code and the motu-client-python.ini file must be saved in $HOME/motu-client/ folder. A file stored in ./db/dbaseconfig.py contains the spots database reference.

## Usage
```
./getSpotsWindWaves.py
```
This will download the NC files from CMEMS (sea data) and NOAA (wind data), accesses the database and extract data for each single spot, saving the results in separate json files.  
It uses the xarray module for python.

```
./getWavesMaps.py
```

This will download the NC files from CMEMS (if not already downloaded by getSpotsWindWaves.py) and extract graphical maps of wave surface height and wave direction. 
It uses the xarray module for python.

