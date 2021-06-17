#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 16 10:54:59 2021

@author: magnusdr
"""
## motivation here is to be able to import itp sets with multiple measurements in addition to 
## temp, sal and depth. As generalised approach as possible (with some hard-coding of inputs that are 
## assumed to be equivalent for all datasets)
import xarray as xr
import pandas as pd
import numpy as np

file = '/home/magnusdr/Downloads/itp114-files/itp114grd0000.dat'

data = pd.read_table(file, skipfooter = 1, header = None, sep='\s+',
                     engine = 'python')

#print(data)
a0 = np.array([data[0][0],data[1][0],data[2][0],data[3][0]]) #list of ITP number and profile number
a1 = np.array([data[6][0],data[7][0],data[8][0], 0]) #list of general metadata info - hopefully general
#a bit of clean-up required, i.e. remove confusing chars
a00l = list(a0[0])
a00l[0] = ''
a0[0] = ''.join(a00l)
a0m1 = list(a0[-1])
a0m1[-1] = ''
a0[-1] = ''.join(a0m1)
#print(a0,a1) #cleaned, so far a purely cosmetic change - can be used in documentation l8r

data = data.drop([0]) 
data = data.reset_index(drop = True)
#Have to extract metadata before dropping Na - or else we delete columns which may contain data
meta = data.iloc[1].drop(data.columns[[0,1]]).dropna(axis = 0).to_numpy()
print(meta)
print(data)
#data.drop([1])
'''
data = data.dropna(axis = 1)
#print(data)
'''
b = np.zeros(len(data.columns)-1,'U20')
#print(b)
for i in range(len(b)-1):
    b[i] = data[i+2][2]
#print(b)
'''
#data = data.drop([2]) 
data = data.astype(float)
data[0] = data[0].astype(int)
#back to the file format we have in pdtrial_itp_iter.py, potentially with more measurements per profile
#print(str(data[0][1]))
data = data.reset_index(drop = True)
#print(data[1])
#print(data)
data['times'] = pd.to_datetime(data[1], unit = 'D', origin = str(data[0][0]))
data = data.drop(data.columns[[0,1]], axis = 1)
#b[-1] = 'times'
#data.columns = b
#print(data)
'''