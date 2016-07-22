import time
import datetime
import sys
import pymssql
import bisect
import csv

########################
### Define Functions ###
########################

##Rounds time to the nearest interval mark, default is ten seconds
def roundTime(dt=None, roundTo=10):
    if dt == None : dt = datetime.datetime.now()
    seconds = (dt - dt.min).seconds
    # // is a floor division, not a comment on following line:
    rounding = (seconds+roundTo/2) // roundTo * roundTo
    return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)

def find_ge(a, x):
    'Find leftmost item greater than or equal to x'
    i = bisect.bisect_left(a, x)
    if i != len(a):
        return a[i]
    return 0

def find_le(a, x):
    'Find rightmost value less than or equal to x'
    i = bisect.bisect_right(a, x)
    if i:
        return a[i-1]
    return 0

##Prints out grid file for selected attribute, emptyNum is what to print if there is no value, default is -1
def createDanDict(attDict, timeArr, headingDict, emptyNum=-1):
    danDict = {}
    for key in headingDict:
        (boxNum,direction) = key
        id = str(boxNum) + ":" + str(direction)
        danDict[id] = []
        for curTime in timeArr:
            try:
                danDict[id].append(str(attDict[(boxNum,direction,str(curTime))]))
            except:
                danDict[id].append(str(emptyNum))
    return danDict

def buildGridDict(boxesFile):
    BOXFILE = open(boxesFile, 'r')
    gridDict = {}
    boxList = []
    latList = []
    lonList = []
    header = True
    for line in BOXFILE:
        if header:
            header = False
            continue
        row = line.strip().split(",")
        boxList.append(row[5])
        lat1 = float(row[1])
        lat2 = float(row[3])
        lon1 = float(row[2])
        lon2 = float(row[4])
        gridDict[(lat1, lon1, lat2, lon2)] = row[5]

    BOXFILE.close()
    return (gridDict, boxList)

def buildLatandLonLists(rightSide, leftSide, topSide, bottomSide):
    boxHeights = (topSide - bottomSide)/1501
    boxWidths = (rightSide - leftSide)/1887 

    latList = [topSide,]
    curLat = topSide
    i = 0
    while i < 1501:
     curLat = curLat - boxHeights
     curLat = round(curLat, 7)
     latList.append(curLat)
     i+=1

    lonList = [leftSide,]
    curLon = leftSide
    i = 0
    while i < 1887:
        curLon = curLon + boxWidths
        curLon = round(curLon, 7)
        lonList.append(curLon)
        i+=1
    return (latList, lonList)

def calc_bottleneck_index(speed, freeflow):
    if speed >= ((2.0/3.0)*float(freeflow)):
        return 1
    if (0.5*float(freeflow)) < speed <= ((2.0/3.0)*float(freeflow)):
        return 2
    if ((1.0/3.0)*float(freeflow)) < speed <= (0.5*float(freeflow)):
        return 3
    if speed <= ((1.0/3.0)*float(freeflow)):
        return 4
    
    return -1

#get reference speeds, return dict of boxid to reference speed
def get_reference_speed(TMC_Box_Ref):
     
    with open(TMC_Box_Ref, 'r') as fp:
        next(fp)
        reader = csv.reader(fp)
        reference_dict = {rows[5]:int(rows[6]) for rows in reader}

    fp.close()
    
    return reference_dict

def createTMCDictionary(TMC_Box_Ref):
    tmcDict = {}
    header_flag = True
    with open(TMC_Box_Ref, 'r') as fp:
        for line in fp:
            if header_flag:
                header_flag = False
                continue
            row = line.strip().split(',')
            if row[0] not in tmcDict.keys():
                tmcDict[row[0]] = []
            tmcDict[row[0]].append([row[5], int(row[7])])
    return tmcDict


def grid_query(argv):
    ######################################################
    ### Get command line inputs and convert to gentime ###
    ######################################################
    try:
        startDate = argv[0]
        startTime = argv[1]
        endDate = argv[2]
        endTime = argv[3]
        interval = int(argv[4])
        boxesFile = argv[5]
        countsoutputfile = argv[6]
        TMC_Box_Ref = argv[7]
    except:
        print "Required input: StartDate(YYYY-MM-DD) StartTime(HH:MM) EndDate(YYYY-MM-DD) EndTime(HH:MM) Interval(seconds) BoxesFile"
        sys.exit(1)
    
    try:
        startDate = datetime.datetime.strptime(startDate, "%Y-%m-%d").date()
    except:
        print "Start Date format is incorrect, correct format is YYYY-MM-DD"
        sys.exit(1)
    try:
        endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d").date()
    except:
        print "End Date format is incorrect, correct format is YYYY-MM-DD"
        sys.exit(1)
    try:
        startTime = datetime.datetime.strptime(startTime, '%H:%M').time()
    except:
        print "Start Time format is incorrect, correct format is militay time HH:MM"
        sys.exit(1)
    try:
        endTime = datetime.datetime.strptime(endTime, '%H:%M').time()
    except:
        print "Start Time format is incorrect, correct format is militay time HH:MM"
        sys.exit(1)
    
    startDateTime = datetime.datetime.combine(startDate, startTime)
    endDateTime = datetime.datetime.combine(endDate, endTime)
    

    ##########################
    ### Get Time Intervals ###
    ##########################
    
    intervalCount = 0
    timeArr = []
    
    now = startDateTime
    end = endDateTime
    while now <= end:
        intervalCount += 1
        timeArr.append(now)
        now += datetime.timedelta(seconds=interval)
    
    halfwayTime = timeArr[len(timeArr)/2 - 1]
    
    ##################################
    ### Get Data from the Database ###
    ##################################
    t0 = time.time()
    print "Connecting to database"
    #The following lines should be uncommented and used to connect to INRIX average speed data
    '''
    conn = pymssql.connect()
    cur = conn.cursor()
    
    monthNum = startDate.month
    
    if monthNum == 4:
        print "Executing query SELECT tmc, measurement_tstamp, speed FROM [dbo].[April_2013_Vehicles] WHERE measurement_tstamp > %s AND measurement_tstamp < %s", (startDateTime.strftime("%Y-%m-%d %H:%M:%S"), endDateTime.strftime("%Y-%m-%d %H:%M:%S"))
        cur.execute("USE InrixDataset SELECT tmc, convert(varchar(20),measurement_tstamp,120), speed FROM [dbo].[April_2013_Vehicles] WHERE measurement_tstamp > %s AND measurement_tstamp < %s", (startDateTime.strftime("%Y-%m-%d %H:%M:%S"), endDateTime.strftime("%Y-%m-%d %H:%M:%S")))
    elif monthNum == 10:
        print "Executing query SELECT tmc, measurement_tstamp, speed FROM [dbo].[October_2012_Vehicles] WHERE measurement_tstamp > %s AND measurement_tstamp < %s", (startDateTime.strftime("%Y-%m-%d %H:%M:%S"), endDateTime.strftime("%Y-%m-%d %H:%M:%S"))
        cur.execute("USE InrixDataset SELECT tmc, convert(varchar(20),measurement_tstamp,120), speed FROM [dbo].[October_2012_Vehicles] WHERE measurement_tstamp > %s AND measurement_tstamp < %s", (startDateTime.strftime("%Y-%m-%d %H:%M:%S"), endDateTime.strftime("%Y-%m-%d %H:%M:%S")))
    else:
        print "Currently data only exists for April 2013 and October 2012, please pick a date from one of those months"
        sys.exit(1)

    t1 = time.time()
    print 'elapsed=%g' % (t1 - t0)
    '''
    ########################
    ### Process the data ###
    ########################
    headingDict = {}
    speedDict = {} #Key is Box,Heading,Time
    boxList = []
    tmcDict = createTMCDictionary(TMC_Box_Ref)
    errorCount = 0
    outsideCount = 0
    lineCount = 0
    print "Processing results"
    for row in cur:
        lineCount+=1
        try:
            speed = float(row[2])
            rowTimeString = row[1][0:16]
            rowTimeString += ":00"
            boxNum, direction = tmcDict[row[0].strip()][0]
            boxList.append(boxNum)
            headingDict[(boxNum,direction)] = 0
            ##Handle the headings
            if (boxNum,direction,rowTimeString) not in speedDict:
                speedDict[(boxNum,direction,rowTimeString)] = speed
            else:
                speedDict[(boxNum,direction,rowTimeString)] = (speedDict[(boxNum,direction,rowTimeString)] + speed)/2
        except:
            errorCount += 1
    print "Lines: " + str(lineCount)
    print "Outside: " + str(outsideCount)
    print "Errors: " + str(errorCount)

    danDict = createDanDict(speedDict, timeArr, headingDict, -1)

    reference = get_reference_speed(TMC_Box_Ref)
    startTime = str(startTime)
    endTime = str(endTime)
    startTime = startTime.replace(':', '')
    endTime = endTime.replace(':', '')
    file_name = 'counts_' + str(startDate) + '_' + startTime + '_' + str(endDate) + '_' + endTime + '.csv'
    with open(countsoutputfile, 'a') as out:
        out.write("%s\n" % (file_name))
    with open(file_name, 'w') as out_f:
        for (boxNum,direction,rowTimeString) in speedDict:
            avg_speed = speedDict[(boxNum,direction,rowTimeString)]
            out_f.write("%s,%s,%d,%g,%d\n" % (boxNum, rowTimeString, direction, avg_speed, calc_bottleneck_index(avg_speed, reference[boxNum])))

    return danDict