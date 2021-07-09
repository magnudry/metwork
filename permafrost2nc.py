#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  8 12:53:58 2021

@author: magnusdr
"""
#The following three variables are global and must be specified in command line 
#starttime = "2021-06-19T12%3A00%3A00.000Z" #strange format 
#endtime = "2021-06-20T00%3A00%3A00.000Z" #specifiy in command line
#path = "/home/magnusdr/Documents/metwork/frostcfg.cfg" #use your own path!
import os
import sys
import argparse
#import csv
import requests
import pandas as pd
#from io import StringIO
import xarray as xr
#import matplotlib.pyplot as plt
#import datetime as dt
import json
import yaml
import logging
#from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

def parse_arguments():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-c","--cfg",dest="cfgfile",
            help="Configuration file", required=True)
    parser.add_argument("-s","--startday",dest="startday",
            help="Start day in the form YYYY-MM-DD", required=True)
    parser.add_argument("-e","--endday",dest="endday",
            help="End day in the form YYYY-MM-DD", required=True)
    args = parser.parse_args()

    try:
        datetime.strptime(args.startday,'%Y-%m-%d')
    except ValueError:
        raise ValueError
    try:
        datetime.strptime(args.endday,'%Y-%m-%d')
    except ValueError:
        raise ValueError

    if args.cfgfile is None:
        parser.print_help()
        parser.exit()

    return args

def parse_cfg(cfgfile):
    # Read config file
    print("Reading", cfgfile)
    with open(cfgfile, 'r') as ymlfile:
        cfgstr = yaml.full_load(ymlfile)

    return cfgstr

def initialise_logger(outputfile = './log'):
    # Check that logfile exists
    logdir = os.path.dirname(outputfile)
    if not os.path.exists(logdir):
        try:
            os.makedirs(logdir)
        except:
            raise IOError
    # Set up logging
    mylog = logging.getLogger()
    mylog.setLevel(logging.INFO)
    #logging.basicConfig(level=logging.INFO, 
    #        format='%(asctime)s - %(levelname)s - %(message)s')
    myformat = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(myformat)
    mylog.addHandler(console_handler)
    file_handler = logging.handlers.TimedRotatingFileHandler(
            outputfile,
            when='w0',
            interval=1,
            backupCount=7)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(myformat)
    mylog.addHandler(file_handler)

    return(mylog)

"""
Times thus far only given by date, w/o time of day
startday = "YYYY-MM-DD"
endday = "YYYY-MM-DD"
"""
def extractdatajson(frostcfg,station,stmd,output):
    clid = frostcfg["client_id"] #ok
    #source = list(station)[0]
    element = "soil_temperature" #only element in permafrost sets, could generalise
    
    #Only fetch json-files
    #url = ("{}{}{}{}{}{}{}{}{}".format(frostcfg["endpointobs"],"?sources=",source,
    #                                   "&referencetime=",args.startday,"%2F",args.endday,"&elements=",element))
    #metaurl = ("{}{}{}{}{}".format(frostcfg["endpointmeta"],"?ids=",source,"&elements=",element))
    #print(url,"\n",metaurl) #check links
    mpar = {"sources" : station, "elements" : element,} 
    par = {"sources" : station, "elements" : element, 
           "referencetime" : ("{}{}{}".format(args.startday,"/",args.endday)),}
    mylog.info("Retrieving metadata for station: %s", station)
    try:
        sourcer = requests.get(frostcfg["endpointmeta"], mpar, auth = (clid,"")) #request sources dataset
    except:
        mylog.error("Something went wrong extracting source metadata.")
        raise
    sourcedata = json.loads(sourcer.text) #sources dict, this is from the metaurl
    # Check that the station has data in the period requested.
    # Sometimes this will fail anyway since there is no data due to technical issues and the station is still considered active.
    if "validTo" in sourcedata["data"][0].keys() and datetime.strptime(args.startday,"%Y-%m-%d") > datetime.strptime(sourcedata["data"][0]["validTo"],"%Y-%m-%dT%H:%M:%S.%fZ"):
        mylog.warn("Station %s doesn't contain data as late as this.", station)
        return
    if "validFrom" in sourcedata["data"][0].keys() and datetime.strptime(args.endday,"%Y-%m-%d") < datetime.strptime(sourcedata["data"][0]["validFrom"], "%Y-%m-%dT%H:%M:%S.%fZ"):
        mylog.warn("Station %s doesn't contain data as early as this.",station)
        return
    mylog.info("Retrieving data for station: %s", station)
    try:
        r = requests.get(frostcfg["endpointobs"], par, auth = (clid,"")) #request observation dataset
    except:
        mylog.error("Something went wrong extracting observation data.")
        raise
    # Check if the request worked, print out any errors
    if r.status_code == 412:
        mylog.error("Information returned indicates that no data is available for this time period for station %s", station)
        return
    if r.status_code != 200:
        mylog.error("Returned status code was %s\nmessage:\n%s", r.status_code, r.text)
        raise
    metadata = json.loads(r.text) #observation dict, confusing name (sorry)
    data = metadata.pop('data') #obtain data dict
    
    df = pd.json_normalize(data, "observations",["sourceId", "referenceTime"]) #dataframe from the json
    #the dataframe needs a bit of work, save the one-off values
    sourceId = df["sourceId"][0] #specified in request
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
    df.drop(["sourceId","elementId","unit","timeOffset","timeResolution","timeSeriesId",
             "performanceCategory", "exposureCategory","qualityCode","level.levelType",
             "level.unit"], axis = "columns", inplace = True)
    #pivot the df to reach the correct format
    df = df.pivot(index = "referenceTime", columns = "level.value", values = "value")
    
    #CF demands single profiles per index in file, thus have to iterate over single rows and extract
    #costly process, please see if possible to optimise
    profilenr = 0 #no profilenr in metadata
    first = True
    for i in range(len(df)):
        #series approach
        si = df.iloc[i] #each row as series, potentially slow approach
        dfi = pd.DataFrame({"depth" : si.index, "soil_temperature" : si.values}) #this is what takes time (?)
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
    #ds.attrs["name"] = "temperature" #what's the purpose of this?
    ds.attrs["standard_name"] = "soil_temperature"
    ds.attrs["units"] = "degree_celsius"
    ds.depth.attrs["standard_name"] = "depth"
    ds.depth.attrs["long_name"] = "depth below surface in centimeters"
    ds.depth.attrs["units"] = "centimeters"
    #global attributes
    ds.attrs['title'] = ("{} {}".format("Permafrost borehole measurements from station", 
                                        sourcedata['data'][0]['shortName']))
    ds.attrs["summary"] = output["abstract"]
    ds.attrs["license"] = metadata["license"]
    ds.attrs['featureType'] = "timeSeriesProfile"
    ds.attrs["time_coverage_start"] = ds.time[0].strftime('%Y-%m-%dT%H:%M:%SZ')
    ds.attrs["time_coverage_end"] = ds.time[-1].strftime('%Y-%m-%dT%H:%M:%SZ')
    ds.attrs["geospatial_lat_min"] = sourcedata["data"][0]["geometry"]["coordinates"][1]
    ds.attrs["geospatial_lat_max"] = sourcedata["data"][0]["geometry"]["coordinates"][1]
    ds.attrs["geospatial_lon_min"] = sourcedata["data"][0]["geometry"]["coordinates"][0]
    ds.attrs["geospatial_lon_max"] = sourcedata["data"][0]["geometry"]["coordinates"][0]
    ds.attrs["creator_name"] = stmd["PrincipalInvestigator"] 
    ds.attrs["creator_email"] = stmd["PrincipalInvestigatorEmail"]
    ds.attrs["creator_url"] = stmd["PrincipalInvestigatorOrganisationURL"]
    ds.attrs["creator_institution"] = stmd["PrincipalInvestigatorOrganisation"]
    ds.attrs["keywords"] = """EARTH SCIENCE > CRYOSPHERE > FROZEN GROUND > PERMAFROST > PERMAFROST TEMPERATURE,
                             #EARTH SCIENCE > LAND SURFACE > SOILS > SOIL TEMPERATURE"""
    ds.attrs["keywords_vocabulary"] = "GCMD"
    ds.attrs["publisher_name"] = ""
    ds.attrs["publisher_email"] = "adc@met.no"
    ds.attrs["publisher_url"] = "https://adc.met.no/"
    ds.attrs["publisher_institution"] = "Norwegian Meteorological Institute"
    ds.attrs["conventions"] = "ACDD 1.3, CF-1.8"
    ds.attrs["date_created"] = metadata["createdAt"]
    ds.attrs["history"] = ("{} {}".format(metadata["createdAt"],
                                          """Data extracted from the MET Observation Database
                                             through Frost and stored as NetCDF-CF"""))
    ds.attrs["source"] = "Soil temperature from permafrost boreholes"
    ds.attrs["wigosId"] = sourcedata["data"][0]["wigosId"]
    ds.attrs["project"] = stmd["project"]
    #to netcdf
    datasetstart4filename = ds.time[0].strftime('%Y%m%d')
    datasetend4filename = ds.time[-1].strftime('%Y%m%d')
    outputfile = ("{}{}{}{}{}{}{}{}".format(output["destdir"],"/",stmd["filename"],
                                    "_",datasetstart4filename,"-",datasetend4filename,".nc"))
    ds.to_netcdf(outputfile,
                 encoding={'depth': {'dtype':'int32'},
                           'time': {'dtype': 'int32'},
                           'soil_temperature': {'dtype': 'float32'}
                           })
    return
"""
args = parse_arguments()
cfgstr = parse_cfg(args.cfgfile)
mylog = initialise_logger(cfgstr["output"]["logfile"])
extractdatajson(cfgstr["frostcfg"],)
"""
if __name__ == "__main__":
    
    # Parse command line arguments
    try:
        args = parse_arguments()
    except:
        raise SystemExit("Command line arguments didn't parse correctly.")

    # Parse configuration file
    cfgstr = parse_cfg(args.cfgfile)

    # Initialise logging
    mylog = initialise_logger(cfgstr["output"]["logfile"])
    mylog.info("Configuration of logging is finished.")

    # Loop through stations
    mylog.info("Process stations requested in configuration file.")
    for station,content in cfgstr["stations"].items():
        mylog.info("Requesting data for: %s", station)
        try:
            extractdatajson(cfgstr["frostcfg"], station, content, cfgstr["output"])
        except:
            raise SystemExit()