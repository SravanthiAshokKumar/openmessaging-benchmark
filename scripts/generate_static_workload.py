import argparse
import math
import matplotlib.pyplot as plt
import random

# Used the derivation given here - https://math.stackexchange.com/questions/1039482/how-to-evenly-space-a-number-of-points-in-a-rectangle
def placePointsInGrid(h, w, n, minX, minY):
    nx = math.sqrt((w*n/h) + ((w-h)**2/float(4)*h**2) - ((w-h)/float(2)*h))
    ny = n/nx
    diff = (w/(nx-1))
    xCoords = []
    yCoords = []

    nx = math.floor(nx)
    ny = math.ceil(ny)
    for i in range(nx):
        xCoords.append(minX + i*diff)
    for j in range(ny):
        yCoords.append(minY + j*diff)

    return xCoords, yCoords

# Idea taken from this post - https://blogs.sas.com/content/iml/2011/01/28/random-uniform-versus-uniformly-spaced-applying-statistics-to-show-choir.html
def adjustPoints(xCoords, yCoords):
    delta = 0.05
    xAdjusted = []
    yAdjusted = []
    for x in xCoords:
        for y in yCoords:
            angle = 2*math.pi*random.uniform(1.0, 10.0)
            xPoint = x + delta*math.cos(angle)
            yPoint = y + delta*math.sin(angle)
            xAdjusted.append(xPoint)
            yAdjusted.append(yPoint)
    return xAdjusted, yAdjusted

def generateLocationsData(xAdjusted, yAdjusted, outputFilename, clients, iterations):
    time = 1.0
    with open(outputFilename, 'w+') as outfile:
        for i in range(iterations):
            c = 0
            for p in range(len(xAdjusted)): 
                outfile.writelines("{} {} {} {}\n".format(clients[c], xAdjusted[p], yAdjusted[p], time))
                c += 1
            time += 1

def main(numClients, iterations, minX, minY, maxX, maxY, outputFilename):
    h = maxY - minY
    w = maxX - minX
    xCoords, yCoords = placePointsInGrid(h, w, float(numClients), minX, minY)
    xAdjusted, yAdjusted = adjustPoints(xCoords, yCoords)
    numClients = len(xCoords)*len(yCoords)
    clients = []
    for i in range(numClients):
        clients.append("car_{}".format(i))

    generateLocationsData(xAdjusted, yAdjusted, outputFilename, clients, iterations)
    plt.scatter(xAdjusted, yAdjusted)
    plt.savefig(outputFilename+".png")
    
def generate_more_pockets(numClients, iterations, minX, minY, maxX, maxY, outfile):
    h = maxY - minY
    w = maxX - minX
    xCoords, yCoords = placePointsInGrid(h, w, float(numClients/4), minX, minY)
    print(xCoords, yCoords)
    clientID = 'car_'
    mod = math.floor(numClients/4)
    time = 1.0
    with open(outfile, 'a+') as of:
        for i in range(iterations):
            c = 0
            while c < numClients:
                for x in xCoords:
                    for y in yCoords:
                        of.writelines("{} {} {} {}\n".format(clientID+str(c), x,
                            y, time))
                        c += 1
            time += 1


def generate_lat_long(numClients, iterations, X1, Y1, X2, Y2, outfile, start_time):
    clientID = 'car_'
    with open(outfile, 'a+') as of:
        for i in range(iterations):
            for c in range(numClients):
                of.writelines("{} {} {} {}\n".format(clientID+str(c), X1, Y1, start_time))
                of.writelines("{} {} {} {}\n".format(clientID+str(c+numClients), X2, Y2,\
                    start_time))
            start_time += 1

def generate_static_data(numClients, iterations, minX, minY, maxX, maxY, outfile):
    constant_x = (maxX - minX)/4.0
    constant_y = (maxY - minY)/4.0
    generate_lat_long(math.ceil(numClients/2), iterations, minX+constant_x,
        minY+constant_y, maxX-constant_x, maxY-constant_y, outfile, 1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--numClients", help="number of clients")
    parser.add_argument("-i", "--iterations", help="number of iterations")
    parser.add_argument("-x", "--minX", help="minimum X coordinate")
    parser.add_argument("-y", "--minY", help="minimum Y coordinate")
    parser.add_argument("-X", "--maxX", help="maximum X coordinate")
    parser.add_argument("-Y", "--maxY", help="maximum Y coordinate")
    parser.add_argument("-o", "--outputFilename", help="filename to generate the output file")

    args = parser.parse_args()

    # generate_static_data(int(args.numClients), int(args.iterations), float(args.minX), float(args.minY),
    #      float(args.maxX), float(args.maxY), args.outputFilename)
    generate_more_pockets(int(args.numClients), int(args.iterations), float(args.minX), 
        float(args.minY), float(args.maxX), float(args.maxY), args.outputFilename)