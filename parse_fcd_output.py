#!/usr/bin/python

import xml.etree.ElementTree as ET
import sys 
from os.path import join
import time, argparse

minLat = 100000
maxLat = 0
minLng = 100000
maxLng = 0

def get_ts_from_timestamp_tag(tag):
    low = tag.find("\"")
    high = tag.find("\"", low+1)
    return float(tag[low+1:high])

def findMinMaxLatLong(lat, lng):
    global minLat, maxLat, minLng, maxLng
    if lat < minLat:
        minLat = lat
    elif lat > maxLat:
        maxLat = lat    
    if lng < minLng:
        minLng = lng
    elif lng > maxLng:
        maxLng = lng

def parse_vehicle_tag(line):
    root = ET.fromstring(line)
    a = root.attrib
    return (float(a["x"]), float(a["y"]), a["id"])

def main(input_file, outfile, low_time, high_time):
    curr_ts = None
    count=0
    low_ts = 0
    high_ts = 100000000
    start_time = low_time
    if low_time and low_time > 0:
        low_ts = low_time
    if high_time and high_time > 0:
        high_ts = high_time
    out = open(outfile, "w")

    batch_size = 0
    batch_str = ""
    with open(input_file) as f:
        line = f.readline()
        while line:
            if "<timestep time=" in line:
                curr_ts = get_ts_from_timestamp_tag(line)
                count = 0
                if int(curr_ts)%1000 == 0:
                    print ("Processed upto %d seconds"%(int(curr_ts)))
            if "</timestep>" in line:
                assert(curr_ts != None)
                curr_ts = None
            if "<vehicle id" in line:
                if curr_ts >= low_ts and curr_ts <= high_ts:
                    assert (curr_ts != None)
                    curr_ts -= low_time
                    lat,lng,vid = parse_vehicle_tag(line)
                    findMinMaxLatLong(lat, lng)
                    batch_str += "%s\t%f\t%f\t%f\n"%(vid, lat, lng, curr_ts)
                    batch_size += 1
                    if batch_size % 500 == 0:
                        out.write(batch_str)
                        batch_size = 0
                        batch_str = ""
                    #out.write("%s\t%f\t%f\t%f\n"%(vid, lat, lng, curr_ts)) 
            try:
                line = f.readline()
            except:
                break
        if batch_size > 0:
            out.write(batch_str)
            batch_size = 0
            batch_str = ""
    out.close()
    return minLat, minLng, maxLat, maxLng

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-O", dest="outfile", help="Output file name")
    parser.add_argument("--fcd-output", dest="fcd_output", type=str, help="FCD output of SUMO. Should contain lat-lng")
    parser.add_argument("--low-time", dest="low_time", type=int, help="Lower bound in simulation time")
    parser.add_argument("--high-time", dest="high_time", type=int, help="Upper bound in simulation time")

    args = parser.parse_args()

    main(args.fcd_output, args.outfile, args.low_time, args.high_time)
