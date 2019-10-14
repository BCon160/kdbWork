#File: loadCSV.py
#Author: Brendan Connolly
#Description: Takes a csv file and publishes the contents to a kdb+ tickerplant (in accordance with Advanced kdb+ CMTP).  Limited impementation, will not handle complex kdb+ types
#Date: 28/12/17

import argparse
import sys
from pyq import q
from datetime import date
from datetime import timedelta

def main():
    #Provide help if required
    if(sys.argv[1] == "-help"):
        print("Usage: pyq loadCSV.py filePath tableName \n\t Required Args:\n\n\t filePath -> Path to csv for loading\n\n\t tableName -> Name of table in kdb+\n\n\t Optional Args:\n\n\t --tpPort -> Port on which kdb+ tickerplant is running (default is 5010)\n\n\t --head -> If the headers are given in the csv file, provide this flag\n")
        exit(0)

    #Parse the arguments passed in from command line
    parser = argparse.ArgumentParser(description='Load a csv and publish the contents to a kdb+ ticker plant')
    parser.add_argument('filePath', help='Path to file for publishing')
    parser.add_argument('tableName', help='Name of table to publish to')
    parser.add_argument('-tpPort', help='Port number for kdb tickerplant process', default=':5010')
    parser.add_argument('-head', action='store_const', const=True, default=False, help='Specify if a headers column is present in csv')
    args = parser.parse_args()

    #Open a handle to kdb+ tp process
    tpHandle = q.hopen(':'+args.tpPort)
    schemaInfo = tpHandle("tables[]!{exec t from x} each meta each tables[]".encode())

    #Open csv file
    openFile = open(args.filePath, mode='r')
    #If headers are present, discard them
    if args.head:
        openFile.readline()
    line = openFile.readline()
    #Until we reach EOF
    while line:
        lineCount = 0
        chunk = []
        #Process the input File in chunks of 100 lines
        while lineCount < 100:
            if not line:
                break
            lineCount+=1
            #Drop the \n from the line
            line = line[:-1]
            chunk.append(line.split(","))
            line = openFile.readline()
        #Take transpose of chunk
        chunk = list(map(list, zip(*chunk)))
        #Cast each of the columns to the correct type from string
        tableSchema = schemaInfo(args.tableName)
        for i in range(len(chunk)):
            castSym = tableSchema[i]
            chunk[i] = cast(castSym, chunk[i])
        #Change data to q form
        q.data = chunk
        #Publish chunk to tp
        tpHandle(['.u.upd', args.tableName, q.data])

#Casts a list from string to the correct type for kdb+
#Params:
#  castSym - Indicates the target type
#  lst - list to be casted to target type
#Returns: The input list converted to the correct type
#Note: This function is quite limited and could be easily broken.  It only deals with fairly limited q data types
def cast(castSym, lst):
    #Floats and reals conversion
    if castSym in ['e', 'f']:
        lst = list(map(float, lst))
    #Long, int, short conversion
    elif castSym in ['h', 'i', 'j']:
        lst = list(map(int, lst))
    #String Conversion
    elif castSym == 'C':
        lst = [x.encode() for x in lst]
    #Date conversion
    elif castSym == 'd':
        lst = [date(*x.split('.')) for x in lst]
    #Timespan conversion
    elif castSym == 'n':
        newVec = []
        for item in lst:
            item = item[2:]
            item = item.replace(':', '.').split('.')
            item = timedelta(days=0,seconds=int(item[0])*3600+int(item[1])*60+int(item[2]),milliseconds=int(item[3][:-6]))
            newVec.append(item)
        lst = newVec
    else:
        pass
    return(lst)

main()
