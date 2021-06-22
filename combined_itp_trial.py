#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 18 11:16:17 2021

@author: magnusdr
"""

import xarray as xr
import pandas as pd
import numpy as np
import datetime as dt
import glob
import time

#can either open entire file once and extract all necessary data into arrays - elegant, maybe faster
#or open twice and keep second row as header - easily read, maybe slightly slower

path = '/home/magnusdr/Downloads/itp114-files' # use your own path!

more_files = sorted(glob.glob(path + "/*.dat"))[:10] # include as many as you want idgaf - 10 for now

first = True
start = time.time()
for i in more_files:
    #one-time opening approach, many lines - much treatment required
    '''
    data = pd.read_table(i, skipfooter = 1, header = None, sep='\s+',  
                     engine = 'python')
    if(data.values.shape[0]<10): # not the other way around?? < --> > ?
        continue
    a0 = np.array([data[0][0],data[1][0],data[2][0],data[3][0]]) #list of ITP number and profile number
    a1 = np.array([data[6][0],data[7][0],data[8][0]]) #list of general metadata variables - maybe unnecessary
    a00l = list(a0[0])
    a00l[0] = ''
    a0[0] = ''.join(a00l)
    a0m1 = list(a0[-1])
    a0m1[-1] = ''
    a0[-1] = ''.join(a0m1) #see gen_itp_trial.py for documentation
    
    print(a0,a1) #cleaned, so far a purely cosmetic change - can be used in documentation l8r
    profile_nr = a0[-1]
    data = data.drop([0]) 
    #Have to extract metadata before dropping Na - or else we delete columns which may contain data
    meta = data.iloc[0].dropna(axis = 0).to_numpy() # obtain general metadata
    var = data.iloc[1].dropna(axis = 0).to_numpy() #obtain data variable names
    print(meta, var)
    data = data.drop([1,2]) #drop top two rows, reindexing happens next
    data = data.reset_index(drop = True)
    data = data.dropna(axis = 1, how ='all')
    
    meta = meta.astype(float)
    meta[0] = meta[0].astype(int) #perhaps problematic for numpy arrays? syntax works for dataframe
    #time in days since 1970-01-01, apparently CF
    measurement_time = pd.to_datetime(meta[1], unit = 'D', origin = str(meta[0])) - dt.datetime(1970,1,1)
    measurement_lat = meta[3]
    measurement_lon = meta[2]
    
    #now the if-sentence nightmare begins
    data = data.astype(float)
    data.columns = var
    #can interchange the two approaches from here - will rewrite variables to match the two codes!
    df = data
    '''
    #two-time opening approach, fewer lines - less dataset treatment
    df = pd.read_table(i,skiprows=2, delim_whitespace=True,skipfooter=1,engine="python")
    
    
    if(df.values.shape[0]<10): # 
        continue
    
    meta = pd.read_table(i,skiprows=None,sep="\s+",nrows=1,engine="python")
    print(df.values.shape[0], int(str(meta.head().columns[3])[:-1]))
    
    measurement_time = pd.to_datetime(float(meta.values[0,1]),origin=str(int(meta.values[0,0])),unit="D")
    measurement_lat  = float(meta.values[0,2])
    measurement_lon  = float(meta.values[0,3])
    profile_nr = int(str(meta.head().columns[3])[:-1])
    if "%year" in df.columns:
        df["%year"] = df["%year"].astype(int)
        df["times"] = pd.to_datetime(df["day"], unit = 'D', 
                                     origin = str(df["%year"][0])) - dt.datetime(1970,1,1)
        df = df.drop(["%year","day"],axis=1)
    
    if "nobs" in df.columns:
        df = df.drop("nobs",axis=1)
    
    if "dissolved_oxygen" in df.columns:
        df = df.rename(columns={"dissolved_oxygen":"moles_of_oxygen_per_unit_mass_in_sea_water"})
        
    #will have to add more similar if-sentences if we discover more variables
    
    df.rename(columns={"%pressure(dbar)":"sea_water_pressure","pressure(dbar)":"sea_water_pressure",
                       "temperature(C)":"sea_water_temperature",
                       "salinity":"sea_water_salinity"}, inplace=True)
    df = df.set_index("sea_water_pressure")
    
    ds = xr.Dataset.from_dataframe(df)
    
    ds["time"] = measurement_time
    ds["latitude"]  = measurement_lat
    ds["longitude"]  = measurement_lon
    
    ds = ds.assign_coords(profile=profile_nr)
    ds = ds.expand_dims("profile")
    
    # joining files
    if first==True:
        buoy= ds
        first=False
    else:
        buoy=xr.concat([buoy,ds],dim = "profile")
    
print("Det tok", time.time()-start)

#La oss fikse litt metadata da
#legg til alle de her greiene: https://adc.met.no/node/4
#lager metadata:
buoy.time.attrs["standard_name"] = "time"
buoy.time.attrs["units"] = "seconds since 1970-01-01 00:00:00+0"# husk å faktisk endre til dette formatet
buoy["sea_water_temperature"].attrs["standard_name"] = "sea_water_temperature"
buoy["sea_water_temperature"].attrs["units"] = "celcius" # husk å bytte til kelvin?
buoy["sea_water_pressure"].attrs["standard_name"] = "sea_water_pressure"
buoy["sea_water_pressure"].attrs["units"] = "dbar"

if "dissolved_oxygen" in df.columns:
    buoy["moles_of_oxygen_per_unit_mass_in_sea_water"].attrs["standard_name"] = "moles_of_oxygen_per_unit_mass_in_sea_water"
    buoy["moles_of_oxygen_per_unit_mass_in_sea_water"].attrs["units"] = "umol/kg"

#global attributes
buoy.attrs["title"] = ("Trajectory of profiles for ITP " + str(meta.head().columns[1])) #change a0 to the meta-indexing
#summary for normal grd-files, Level 2
buoy.attrs["summary"] = ("""Trajectory of ITP (Ice-Tethered Profiler) profiles, that use pressure in dbar as vertical coordinate.
                         All profiles contain measurement times, temperature and salinity, and may include dissolved oxygen,
                         chromophoric dissolved organic matter (CDOM), turbidity, mass concentration of chlorophyll,
                         photosynthetically active radiation (PAR) and velocities. Metadata include time of initialization,
                         coordinates and profile data points (ndepths). 'The Ice-Tethered Profiler data were collected and made 
                         available by the Ice-Tethered Profiler Program (Toole et al., 2011; Krishfield et al., 2008) 
                         based at the Woods Hole Oceanographic Institution (https://www2.whoi.edu/site/itp/).'""")
#summary for final files, averaged
buoy.attrs["summary"] = ("""Trajectory of ITP (Ice-Tethered Profiler) profiles, that use pressure in dbar as vertical coordinate.
                         All profiles contain averaged measurements of temperature and salinity, and may include dissolved oxygen,
                         chromophoric dissolved organic matter (CDOM), turbidity, mass concentration of chlorophyll,
                         photosynthetically active radiation (PAR) and velocities. Metadata include time of initialization,
                         coordinates and profile data points (ndepths). 'The Ice-Tethered Profiler data were collected and made 
                         available by the Ice-Tethered Profiler Program (Toole et al., 2011; Krishfield et al., 2008)
                         based at the Woods Hole Oceanographic Institution (https://www2.whoi.edu/site/itp/).' """)
#keywords må skrives om, med hele pathen til ordet
buoy.attrs["keywords"] = ["EARTH SCIENCE > OCEANS > SALINITY/DENSITY > DENSITY",
                          "EARTH SCIENCE > OCEANS > OCEAN TEMPERATURE > WATER TEMPERATURE",
                          "EARTH SCIENCE > OCEANS > SALINITY/DENSITY > SALINITY",
                          "EARTH SCIENCE > OCEANS > OCEAN CHEMISTRY > OXYGEN",
                          "EARTH SCIENCE > OCEANS > OCEAN CHEMISTRY > ORGANIC MATTER",
                          "EARTH SCIENCE > OCEANS > OCEAN OPTICS > TURBIDITY",
                          "EARTH SCIENCE > OCEANS > OCEAN CHEMISTRY > CHLOROPHYLL",
                          "EARTH SCIENCE > OCEANS > OCEAN CIRCULATION > ADVECTION"]
#buoy.attrs["keywords"] = ["Water Pressure", "Water Temperature", "Salinity", "Photosynthetically Active Radiation", "Turbidity", 
#                         "Oxygen","Chlorophyll", "Organic Matter", "Advection", "Buoy Position"]
buoy.attrs["keywords_vocabulary"] = "GCMD"
buoy.attrs["featureType"] = "trajectoryProfile"

buoy.attrs["geospatial_lat_min"] = min(buoy.latitude.values)
buoy.attrs["geospatial_lat_max"] = max(buoy.latitude.values)
buoy.attrs["geospatial_lon_min"] = min(buoy.longitude.values)
buoy.attrs["geospatial_lon_max"] = max(buoy.longitude.values)

buoy.attrs["time_coverage_start"] = min(buoy.time.values)
buoy.attrs["time_coverage_end"] = max(buoy.time.values)

buoy.attrs["Conventions"] = "ACDD-1.3"
buoy.attrs["history"] = "Nan" #???
buoy.attrs["date_created"] = str(dt.date.today())
buoy.attrs["creator_type"] = "institution" #?
buoy.attrs["creator_institution"] = "Woods Hole Oceanographic Institution (WHOI)"
buoy.attrs["creator_name"] = "Woods Hole Oceanographic Institution"
buoy.attrs["creator_email"] = "information@whoi.edu" #?
buoy.attrs["creator_url"] = "https://www2.whoi.edu/site/itp/data/"
buoy.attrs["project"] = "Nan"
buoy.attrs["license"] = "None"


buoy