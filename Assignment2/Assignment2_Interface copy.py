
import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    c = openconnection.cursor()
    if ratingMinValue <0 or ratingMaxValue>5:
        print ("Input data is invalid")
    c.execute("select * from RangeRatingsMetadata;")
    records = c.fetchall()
    for i in records:
        table = "RangeRatingsPart" + str(i[0])
        if not ((ratingMinValue > i[2]) or (ratingMaxValue < i[1])):
            c.execute(
                "select * from " + table + " where rating >= " + str(ratingMinValue) + " and rating <= " + str(
                    ratingMaxValue) + ";")
            rangeData = c.fetchall()
            with open(outputPath, "a") as file:
                if len(rangeData)!=0:
                    for a in rangeData:
                        file.write(str(table) + "," + str(a[0]) + "," + str(a[1]) + "," + str(a[2]) + "\n")

    c.execute("select partitionnum from RoundRobinRatingsMetadata;")
    partitions = c.fetchall()[0][0]
    for i in range(0, partitions):
        table = "RoundRobinRatingsPart" + str(i)
        c.execute("select * from " + table + " where rating >= " + str(ratingMinValue) + " and rating <= " + str(
            ratingMaxValue) + ";")
        roundRobinData = c.fetchall()
        with open(outputPath, "a") as file:
            if len(roundRobinData)!=0:
                for a in roundRobinData:
                    file.write(str(table) + "," + str(a[0]) + "," + str(a[1]) + "," + str(a[2]) + "\n")


def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    if ratingValue<0 or ratingValue>5:
        print("Input data is invalid")
    c = openconnection.cursor()
    c.execute("select * from RangeRatingsMetadata;")
    records = c.fetchall()
    for row in records:
        table = "RangeRatingsPart" + str(row[0])
        partitionNum=row[0]
        if ((partitionNum == 0 and ratingValue >= row[1] and ratingValue <= row[2]) or (
                partitionNum != 0 and ratingValue > row[1] and ratingValue <= row[2])):
            c.execute("select * from " + table + " where rating = " + str(ratingValue) + ";")
            rangeData = c.fetchall()
            with open(outputPath, "a") as file:
                if len(rangeData)!=0:
                    for a in rangeData:
                        file.write(str(table) + "," + str(a[0]) + "," + str(a[1]) + "," + str(a[2]) + "\n")

    c.execute("select partitionnum from RoundRobinRatingsMetadata;")
    partitions = c.fetchall()[0][0]
    for i in range(0, partitions):
        table = "RoundRobinRatingsPart" + str(i)
        c.execute("select * from " + table + " where rating = " + str(ratingValue) + ";")
        roundrobinData = c.fetchall()
        with open(outputPath, "a") as file:
            if len(roundrobinData)!=0:
                for a in roundrobinData:
                    file.write(str(table) + "," + str(a[0]) + "," + str(a[1]) + "," + str(a[2]) + "\n")
    
