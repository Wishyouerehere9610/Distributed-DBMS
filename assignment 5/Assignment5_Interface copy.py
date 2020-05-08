
#Ziming Dong
#CSE 512
#Assignment 5
#
# Assignment5 Interface
# Name: Ziming Dong
#
import math
from pymongo import MongoClient
import os
import sys
import json
from math import radians

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    cityToSearch = cityToSearch.lower()
    file=open(saveLocation1,'w')
    for record in collection.find():
        if record['city'].lower()==cityToSearch:
            name=record['name']
            full_address=record['full_address']
            city=record['city']
            state=record['state']
            file.write(str(name).upper()+' $ '+str(full_address).upper()+' $ '+str(city).upper()+' $ '+str(state).upper()+' $ '+'\n')
    file.close()

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    cat=collection.find({'categories':{'$in': categoriesToSearch}})
    latitude1=float(myLocation[0])
    longtitude1=float(myLocation[1])
    file=open(saveLocation2,'w')
    for record in cat:
        latitude2=record['latitude']
        longtitude2=record['longitude']
        name = record['name']
        distance=DistanceFunction(float(latitude2),float(longtitude2),latitude1,longtitude1)
        if distance<=maxDistance:
            file.write(str(name).upper()+'\n')
    file.close()


def DistanceFunction(latitude1,longtitude1,latitude2,longtitude2):
    R=3959
    la1 = radians(latitude1)
    la2 = radians(latitude2)
    latDelta = radians((latitude2 - latitude1))
    lonDelta = radians((longtitude2 - longtitude1))
    a = math.sin(latDelta / 2) * math.sin(latDelta / 2) + math.cos(la1) * math.cos(la2) * math.sin(lonDelta/2) * math.sin(lonDelta/2);
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d=R*c
    return d
