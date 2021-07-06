#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 30 11:47:59 2021

@author: magnusdr
"""
import pandas as pd
import requests
import json
import xarray as xr
import yaml
path = "/home/magnusdr/Documents/metwork/frostcfg.cfg" #use your own path!
#cfg reader, returns yaml format dict
def parse_cfg(cfgfile):
    # Read config file
    print("Reading", cfgfile)
    with open(cfgfile, 'r') as ymlfile:
        cfgstr = yaml.full_load(ymlfile)

    return cfgstr
#read cfg file
cfg = parse_cfg(path)
#print(cfg)

clid = cfg["frostcfg"]["client_id"] #ok
source = list(cfg["stations"])[0]

#the following variables decide what data is fetched from frost


starttime = "2021-06-19T12%3A00%3A00.000Z" #strange format 
endtime = "2021-06-20T00%3A00%3A00.000Z" #specifiy in command line
element = "soil_temperature" #only element in permafrost sets, could generalise
#Only fetch json-files
url = ("{}{}{}{}{}{}{}{}{}".format(cfg["frostcfg"]["endpointobs"],"?sources=",source,"&referencetime=",
                             starttime,"%2F",endtime,"&elements=",element))
metaurl = ("{}{}{}{}{}".format(cfg["frostcfg"]["endpointmeta"],"?ids=",source,"&elements=",element))
#print(url,"\n",metaurl) #check links

r = requests.get(url, auth = (clid,"")) #request observation dataset
metar = requests.get(metaurl, auth = (clid,"")) #request metadataset

metadata = json.loads(r.text) #observation dict, confusing name (sorry)
data = metadata.pop('data') #obtain data dict

coorddata = json.loads(metar.text) #sources dict, this is from the sources url

df = pd.json_normalize(data, "observations",["sourceId", "referenceTime"]) #dataframe from the json

#the dataframe needs a bit of work, save the one-off values
sourceId = df["sourceId"][0] #maybe specified in request
elementId = df["elementId"][0] #this as well
tempunit = df["unit"][0]
timeOffset = df["timeOffset"][0]
timeResolution = df["timeResolution"][0]
timeSeriesId = df["timeSeriesId"][0]
perfCat = df["performanceCategory"][0]
expCat = df["exposureCategory"][0]
qualityCode = df["qualityCode"][0] #this can actually change over the course of observations
l_levelType = df["level.levelType"][0]
l_unit = df["level.unit"][0]

#now drop them
df.drop(["sourceId","elementId","unit","timeOffset","timeResolution","timeSeriesId","performanceCategory",
          "exposureCategory","qualityCode","level.levelType","level.unit"], axis = "columns", inplace = True) 
#pivot the df to reach the correct format
df = df.pivot(index = "referenceTime", columns = "level.value", values = "value")
#df = df.pivot(index = "level.value", columns = "referenceTime", values = "value")


#print(df)

#CF demands single profiles per index in file, thus have to iterate over single rows and extract
#costly process, please see if possible to optimise

#df0 = df.iloc[[0]]
#print(df0)
#ds0 = xr.Dataset.from_dataframe(df0)
#print(ds0)

profilenr = 0 #no profilenr in metadata
first = True
for i in range(len(df)):
    #series approach
    si = df.iloc[i] #each row as series, potentially slow approach
    dfi = pd.DataFrame({"depth" : si.index, "temperature" : si.values}) #this is what takes time (?)
    dfi = dfi.set_index("depth")
    prof = xr.Dataset.from_dataframe(dfi) #xarray needs to take in a dataframe
    
    #dataframe approach
    #dfi = df.iloc[[i]]
    
    #potential dictionary approach?
    
    prof["time"] = df.index[i]
    prof = prof.assign_coords(profile = profilenr)
    prof = prof.expand_dims("profile")    
    #join sets
    if first:
        ds = prof
        first = False
    else:
        ds = xr.concat([ds,prof], dim = "profile") #similar to append in time consumption??
    profilenr += 1


ds.attrs["name"] = "temperature"
ds.attrs["standard_name"] = "soil_temperature"
ds.attrs["units"] = "degree_celsius"
ds.depth.attrs["standard_name"] = "depth"
ds.depth.attrs["long_name"] = "depth below surface in centimeters"
ds.depth.attrs["units"] = "centimeters"
#global attributes
ds.attrs['title'] = ("{} {}".format("Permafrost borehole measurements from station", 
                                    coorddata['data'][0]['shortName']))
ds.attrs["summary"] = "Nan"
ds.attrs['featureType'] = "timeSeriesProfile"
ds.attrs["geospatial_lat_min"] = coorddata["data"][0]["geometry"]["coordinates"][1]
ds.attrs["geospatial_lat_max"] = coorddata["data"][0]["geometry"]["coordinates"][1]
ds.attrs["geospatial_lon_min"] = coorddata["data"][0]["geometry"]["coordinates"][0]
ds.attrs["geospatial_lon_max"] = coorddata["data"][0]["geometry"]["coordinates"][0]
ds.attrs["license"] = metadata["license"]
ds.attrs["conventions"] = "ACDD 1.3, CF-1.8"

ds.attrs["keywords"] = """EARTH SCIENCE > CRYOSPHERE > FROZEN GROUND > PERMAFROST > PERMAFROST TEMPERATURE,
                         #EARTH SCIENCE > LAND SURFACE > SOILS > SOIL TEMPERATURE"""

ds.attrs["keywords_vocabulary"] = "GCMD"
print(ds)

