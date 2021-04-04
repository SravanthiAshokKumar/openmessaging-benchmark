import argparse
import math
import matplotlib.pyplot as plt
import os
import random
import sys
import yaml

# Used the derivation given here - https://math.stackexchange.com/questions/1039482/how-to-evenly-space-a-number-of-points-in-a-rectangle
def placePointsInGrid(h, w, n, min_lat, min_long):
    nx = math.sqrt((w*n/h) + ((w-h)**2/float(4)*h**2) - ((w-h)/float(2)*h))
    ny = n/nx
    diff = (w/(nx-1))
    x_coords_temp = []
    y_coords_temp = []

    nx = math.floor(nx)
    ny = math.ceil(ny)
    for i in range(nx):
        x_coords_temp.append(min_lat + i*diff)
    for j in range(ny):
        y_coords_temp.append(min_long + j*diff)

    x_coords = []
    y_coords = []
    for i in x_coords_temp:
        for j in y_coords_temp:
            x_coords.append(i)
            y_coords.append(j)
    return x_coords, y_coords

# Idea taken from this post - https://blogs.sas.com/content/iml/2011/01/28/random-uniform-versus-uniformly-spaced-applying-statistics-to-show-choir.html
def adjustPoints(x_coords, y_coords, max_lat, max_long):
    delta = 0.05
    x_adjusted = []
    y_adjusted = []
    for x, y in zip(x_coords, y_coords):
        angle = 2*math.pi*random.uniform(1.0, 10.0)
        x_point = x + delta*math.cos(angle)
        if x_point > max_lat:
            x_point = max_lat
        y_point = y + delta*math.sin(angle)
        if y_point > max_long:
            y_point = max_long
        x_adjusted.append(x_point)
        y_adjusted.append(y_point)
    return x_adjusted, y_adjusted

def write_locations(x_coords, y_coords, clients, time, outfile, no_random, interval):
    random_time = []
    if not no_random:
        for p in range(len(x_coords)):
            random_time.append(time + int(random.uniform(0, interval-1)))
        random_time.sort()

    with open(outfile, 'a+') as out:
        c = 0
        for p in range(len(x_coords)):
            if not no_random:
                out.writelines("{} {} {} {}\n".format(clients[c], x_coords[p], y_coords[p], random_time[p]))
            else:
                out.writelines("{} {} {} {}\n".format(clients[c], x_coords[p], y_coords[p], time))
            c += 1

def generate_location_data(numStaticClients, numMobileClients, min_lat, min_long, max_lat, max_long,
    locChangeInterval, totalDuration, outfile):
    h = max_long - min_long
    w = max_lat - min_lat
    time = 1.0
    if numStaticClients > 0:
        x_coords, y_coords = placePointsInGrid(h, w, float(numStaticClients), min_lat, min_long)
        numStaticClients = len(x_coords)*len(y_coords)
        
        static_clients = []
        for i in range(numStaticClients):
            static_clients.append("car_s_{}".format(i))
        write_locations(x_coords, y_coords, static_clients, time, outfile, True, locChangeInterval)

    if numMobileClients > 0:
        x_coords, y_coords = placePointsInGrid(h, w, float(numMobileClients), min_lat, min_long)
        numMobileClients = len(x_coords)*len(y_coords)
        mobile_clients = []
        for i in range(numMobileClients):
            mobile_clients.append("car_m_{}".format(i))
        write_locations(x_coords, y_coords, mobile_clients, time, outfile, True, locChangeInterval)
        
        while time < totalDuration:
            time += locChangeInterval
            x_coords, y_coords = adjustPoints(x_coords, y_coords, max_lat, max_long)
            write_locations(x_coords, y_coords, mobile_clients, time, outfile, False, locChangeInterval)
    
if __name__ == "__main__":
    config_file = sys.argv[1]
    with open(config_file, 'r+') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    
    if os.path.exists(config['outfile']):
        os.remove(config['outfile'])

    generate_location_data(config['numStaticClients'], config['numMobileClients'], config['min_lat'],
        config['min_lng'], config['max_lat'], config['max_lng'], config['locChangeInterval'],
        config['totalDuration'], config['outfile'])