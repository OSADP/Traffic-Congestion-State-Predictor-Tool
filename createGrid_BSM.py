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
    
    
    if speed >= (0.6 * float(freeflow)):
        return 1
    if speed < (0.6 * float(freeflow)):
        return 2  
    
    return -1

#get reference speeds, return dict of boxid to reference speed
def get_reference_speed(TMC_Box_Ref):
    
        
    with open(TMC_Box_Ref, 'r') as fp:
        next(fp)
        reader = csv.reader(fp)
        reference_dict = {rows[5]:int(rows[6]) for rows in reader}

    fp.close()
    
    return reference_dict


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
    
    convertDate = datetime.datetime(year=2004, month=1, day=1)
    
    startGenTime = (startDateTime - convertDate).total_seconds() * 1000000
    endGenTime = (endDateTime - convertDate).total_seconds() * 1000000

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
    #########################################
    ### Build Lat/Lon lists and Grid Dict ###
    #########################################
    
    rightSide = -83.481821
    leftSide = -84.136543
    topSide = 42.032925
    bottomSide = 42.448569
    (gridDict, boxList) = buildGridDict(boxesFile)
    (latList, lonList) = buildLatandLonLists(rightSide, leftSide,topSide,bottomSide)
    
    ##################################
    ### Get Data from the Database ###
    ##################################
    t0 = time.time()
    print "Connecting to database"
    #The following lines should be uncommented and used to connect to BSM data
    '''
    conn = pymssql.connect()
    cur = conn.cursor()
    
    monthNum = startDate.month
    
    if monthNum == 4:
        print "Executing query USE BsmApr2013 SELECT Gentime, Latitude, Longitude, Speed, Heading, Ay FROM [dbo].[BSMP1] WHERE gentime > %s AND gentime < %s AND speed < 29.05" % (startGenTime, endGenTime)
        cur.execute("USE BsmApr2013 SELECT Gentime, Latitude, Longitude, Speed, Heading, Ay FROM [dbo].[BSMP1] WHERE gentime > %s AND gentime < %s AND speed < 29.05", (startGenTime, endGenTime))
    elif monthNum == 10:
        print "Executing query USE BsmOct2012 SELECT Gentime, Latitude, Longitude, Speed, Heading, Ay FROM [dbo].[BSMP1] WHERE gentime > %s AND gentime < %s AND speed < 29.05" % (startGenTime, endGenTime)
        cur.execute("USE BsmOct2012 SELECT Gentime, Latitude, Longitude, Speed, Heading, Ay FROM [dbo].[BSMP1] WHERE gentime > %s AND gentime < %s AND speed < 29.05", (startGenTime, endGenTime))
    else:
        print "Currently data only exists for April 2013 and October 2012, please pick a date from one of those months"
        sys.exit(1)
    print startGenTime
    print endGenTime
    t1 = time.time()
    print 'elapsed=%g' % (t1 - t0)
    '''
    ########################
    ### Process the data ###
    ########################
    headingDict = {} #Key is Box,Direction(1 or 2)
    speedDict = {} #Key is Box,Heading,Time
    accelDict = {}
    bsmCountDict = {}
    errorCount = 0
    outsideCount = 0
    gridRowCount = 0
    lineCount = 0
    print "Processing results"
    for row in cur:
        lineCount+=1
        gentimeStr = str(row[0])
        rowGenTime = float(gentimeStr)/1000000
        rowTime = convertDate + datetime.timedelta(seconds=int(rowGenTime))
        #print str(gentimeStr) + ',' + str(rowGenTime) + ',' + str(rowTime)
        rowTime = roundTime(rowTime, interval)
        rowTimeString = str(rowTime)
        lat = float(row[1])
        lon = float(row[2])
        speed = row[3]
        heading = row[4]
        accel = row[5]
        UL_Lat = float(find_ge(latList, lat))
        BR_Lat = float(find_le(latList, lat))
        UL_Lon = float(find_le(lonList, lon))
        BR_Lon = float(find_ge(lonList, lon))
        searchTuple = (BR_Lat,UL_Lon,UL_Lat,BR_Lon)
        try:
            boxNum = gridDict[searchTuple]
            ##Handle the headings
            if (boxNum,1) not in headingDict:
                gridRowCount += 4
                headingDict[(boxNum, 1)] = heading
                if heading < 180:
                    oppositeHeading = heading + 180
                else:
                    oppositeHeading = heading - 180
                headingDict[(boxNum, 2)] = oppositeHeading
                direction = 1
            else:
                headingDiff1 = heading - headingDict[(boxNum,1)]
                headingDiff2 = heading - headingDict[(boxNum,2)]
                if headingDiff1 < headingDiff2:
                    direction = 1
                else:
                    direction = 2
            if (boxNum,direction,rowTimeString) not in speedDict:
                speedDict[(boxNum,direction,rowTimeString)] = speed
            else:
                speedDict[(boxNum,direction,rowTimeString)] = (speedDict[(boxNum,direction,rowTimeString)] + speed)/2
        except:
            if lat < bottomSide or lat > topSide or lon < leftSide or lon > rightSide:
                outsideCount += 1
            else:
                errorCount += 1
    
    ##Fill out the headingDict so that it contains all box/direction combinations
    for boxNum in boxList:
        if (boxNum,1) not in headingDict:
            gridRowCount += 2
            headingDict[(boxNum,1)] = 0
            headingDict[(boxNum, 2)] = 0
    
    danDict = createDanDict(speedDict, timeArr, headingDict, -1)
    
    print "Lines: " + str(lineCount)
    print "Outside: " + str(outsideCount)
    print "Errors: " + str(errorCount)

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
            avg_speed = speedDict[(boxNum,direction,rowTimeString)] * 2.23694
            out_f.write("%s,%s,%d,%g,%d\n" % (boxNum, rowTimeString, direction, avg_speed, calc_bottleneck_index(avg_speed, reference[boxNum])))

    return danDict

