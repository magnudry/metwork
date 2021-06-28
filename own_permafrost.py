#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 24 10:23:23 2021

@author: magnusdr
"""
import pandas as pd
import requests
#from io import StringIO
import json
#import numpy as np
import xarray as xr

#can specify details for extraction in request - use a more general link
#trial with a few different sources
url = "https://frost.met.no/observations/v0.jsonld?sources=SN99763&referencetime=2021-06-19T12%3A00%3A00.000Z%2F2021-06-20T00%3A00%3A00.000Z&elements=soil_temperature"
url1 = "https://frost.met.no/observations/v0.jsonld?sources=SN99927&referencetime=2021-05-20T00%3A00%3A00.000Z%2F2021-06-20T00%3A00%3A00.000Z&elements=soil_temperature"
metaurl1 = "https://frost.met.no/sources/v0.jsonld?ids=SN99927&elements=soil_temperature"
clid = "" #client id, remember to delete id before push
r = requests.get(url1, auth = (clid,"")) 
metar = requests.get(metaurl1, auth = (clid,"")) #get station coords
#print(r.status_code)

metadata = json.loads(r.text)
data = metadata.pop('data')
print(metadata)
#print(data[0])

coorddata = json.loads(metar.text)
    
#df = pd.DataFrame(data)
#sourceId = df['sourceId'][0]
#df = df.drop('sourceId', axis = "columns")
#df = df.set_index("referenceTime")
#want to extract temperature data from the observations column, as well as meta
#try with pandas json_normalize

df = pd.json_normalize(data, "observations",["sourceId", "referenceTime"])
#df = pd.json_normalize(data, ["sourceId", "referenceTime"],"observations") #does not work
#fetch metadata
sourceId = df["sourceId"][0] #maybe specified in request
elementId = df["elementId"][0] #this as well
tempunit = df["unit"][0]
timeOffset = df["timeOffset"][0]
timeResolution = df["timeResolution"][0]
timeSeriesId = df["timeSeriesId"][0]
perfCat = df["performanceCategory"][0]
expCat = df["exposureCategory"][0]
qualityCode = df["qualityCode"][0]
l_levelType = df["level.levelType"][0]
l_unit = df["level.unit"][0]
#some of the metadata may be general and unecessary to save before dropping :)
#drop these bois
df.drop(["sourceId","elementId","unit","timeOffset","timeResolution","timeSeriesId","performanceCategory",
          "exposureCategory","qualityCode","level.levelType","level.unit"], axis = "columns", inplace = True) 

#get columns for each of the measuring points, using reshape - maybe specific.. but helps for understanding
#df = df.groupby(["referenceTime"])
#df.set_index("referenceTime", inplace = True)


#df.rename(columns={"level.value" : "depth"}, inplace = True)
#try pivoting the df
df = df.pivot(index = "level.value", columns = "referenceTime", values = "value")

#YEEEEEESSSSSS!!!! That's how to do it!!

print(df)

#add all metadata/attributes and whatever
ds = xr.Dataset.from_dataframe(df)
#Depth as dimension?
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
                          EARTH SCIENCE > LAND SURFACE > SOILS > SOIL TEMPERATURE"""
ds.attrs["keywords_vocabulary"] = "GCMD"
print(ds)
