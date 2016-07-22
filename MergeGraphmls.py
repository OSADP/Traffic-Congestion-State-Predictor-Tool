import networkx as nx
import datetime
import time
from datetime import datetime, timedelta
import sys

# October
days = ['1','2','3','4','5','6','7','8','9','10','11', '12','13','14','15','16','17', '18','19','20','21','22','23','24','25','26','27','28','29','30','31']

startTime = '09:00'
endTime = '23:59'
def generateTimeSlices(startTime, endTime):
	timeArr = []
	timeSlicesArr = []
	(startHour, startMin) = startTime.strip().split(':')
	(endHour, endMin) = endTime.strip().split(':')
	t = datetime(2000, 1, 1, int(startHour), int(startMin), 0)
	while t < datetime(2000, 1, 1, int(endHour), int(endMin), 0):
		timeArr.append(str(t.time()))
		t = t + timedelta(minutes=10)
	timeArr.append(endTime)
	for i in xrange(0,len(timeArr) - 2, 2):
		startTime = timeArr[i][:5]
		endTime = timeArr[i+2][:5]
		timeTuple = (startTime,endTime)
		timeSlicesArr.append(timeTuple)
	return timeSlicesArr

timeSlices = generateTimeSlices(startTime, endTime)

with open(sys.argv[1], 'w') as out_f:
	for day in days:
		for i in xrange(0,len(timeSlices), 3):
			time1 = timeSlices[i][0].translate(None, ':')
			time2 = timeSlices[i+1][0].translate(None, ':')
			time3 = timeSlices[i+2][0].translate(None, ':')
			time4 = timeSlices[i+2][1].translate(None, ':')
			(timeT, discard) = timeSlices[i+2][1].strip().split(':')
			try:
				print "Reading Graph 1"
				inputgraph1 = nx.read_graphml("2012-10-" + day + "_" + time1 + "_2012-10-" + day +"_"+time2+".graphml")
				print "Reading Graph 2" 
				inputgraph2 = nx.read_graphml("2012-10-" + day + "_" + time2 + "_2012-10-" + day +"_"+time3+".graphml")
				print "Combining Graphs"
				testGraph = nx.disjoint_union(inputgraph1,inputgraph2)
				print "Reading Graph 3" 
				inputgraph3 = nx.read_graphml("2012-10-" + day + "_" + time3 + "_2012-10-" + day +"_"+time4+".graphml")
				print "Combining Graphs"
				testGraph2 = nx.disjoint_union(testGraph,inputgraph3)

				nx.write_graphml(testGraph2, "2012-10-" + day + "T"+ timeT + ".graphml")
				
				out_f.write("%s\n" % ("2012-10-" + day + "T"+ timeT + ".graphml"))
				
			except:
				print "Couldn't generate merged file for 2012-10-" + day

