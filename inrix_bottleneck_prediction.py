import csv
import pymssql
import os
import sys
# use inrix's algorithm for identifying initial bottlenecks using BSM
# combine consecutive and persistent bottlenecks using inrix logic for the BSM data.
# compare current reported speed to reference speed for each segment of the road.
#reference speed = 85th percentile for all time periods
#<60% of reference speed = potential bottleneck
#<60% for 5 minutes = bottleneck
#adjacent road segments meeting condition are joined together to for bottleneck queue
#>60% for 10 minutes = bottleneck clear
# <0.3 miles queue = remove

def main():

    
    filename = sys.argv[1]
    final= bottleneck_queue(filename)
    
    format_output(final,filename)
    
    #for i in speed_list:
        #print i
    return final

def format_output(output,filename):


#Algorithm output:
 
#Start TMC Code, End TMC Code, Start Date of the Bottleneck, Start time of the Bottleneck, End Date of the Bottleneck, End Time of the Bottleneck, Duration, Length of bottleneck queues, 
#TMC1, Start Date, Start Time, End Date, End Time, Duration, TMC2
 

    fp = open(filename[:-4]+"_Bottleneck_Output.csv", "w")
    #fp2 = open(filename[:-4] +"_Ground_Truth_Inrix.csv", "w")
    fp.write("Start_TMC,End_TMC,Bottleneck_Start_Date,Bottleneck_Start_Time,Bottleneck_End_Date,Bottleneck_End_time,Duration,Queue_Length\n")
    #fp2.write("Start_TMC,Bottleneck_Start_Date,Bottleneck_Start_Time,Bottleneck_End_Date,Bottleneck_End_time,Duration,Queue_Length\n")
    length_dict = get_length()
    
    for i in output:

        for j in i:
            end_tmc = j[0][0]
            end_date = j[0][2][:10]
            end_time = j[0][2][11:]
            duration = str(j[0][3])
            length = length_dict[j[0][0]]
            
            flag = False
            
            if len(j) > 1:
                flag = True
                # set stuff
            
            if flag == True:
                
                # make list append rest to the list then join
                #set end_tmc
                #set end_date
                #set end time
                #set duration
                time = (int(j[0][2][11:13])*60) + int(j[0][2][14:16])
                for k in j:
                    time_compare = ((int(k[2][11:13])*60) + int(k[2][14:16]))
                    if time < time_compare:
                        end_time = k[2][11:]
                        end_tmc = k[0]
                        time = time_compare
                    length += length_dict[k[0]]
                    
                    #print index, k
                duration = time+1 - ((int(j[0][1][11:13])*60) + int(j[0][1][14:16]))
                print j[0][0] +"," + end_tmc + "," +j[0][1][:10] + "," + j[0][1][11:] + "," + end_date + "," + end_time +"," + str(duration) + "," + str(length)
                fp.write(j[0][0] + "," + end_tmc + ","+ j[0][1][:10] + "," + j[0][1][11:] + "," + end_date + "," + end_time +"," + str(duration) + "," + str(length) + "\n")
            else:
                
            #printj is length of bottleneck in tmc road segments
                print j[0][0] +","+ end_tmc + "," + j[0][1][:10] + "," + j[0][1][11:] + "," + end_date + "," + end_time +"," + duration +"," + str(length)#do tmc 1 here
                #print j[0][0] +"," + j[0][1][:10] + "," + j[0][1][11:] + "," + end_date + "," + end_time +"," + duration +"," + str(length)
                fp.write(j[0][0] + "," + end_tmc + ","+ j[0][1][:10] + "," + j[0][1][11:] + "," + end_date + "," + end_time +"," + duration +"," + str(length) + "\n") # do tmc 1 here

    fp.close()
    #fp2.close()
    return 0

def average_index(speed_list, tmc_dict):
    
    for i, j in enumerate(speed_list):
        speed_list[i][0] = tmc_dict[j[0]]
        
    ave_list= []
    #date then tmc
    speed_list.sort(key = lambda x: x[3])
    speed_list.sort(key = lambda x: x[0])
    
    id_index = []


    for h,i in enumerate(speed_list):
        if i[0] in speed_list[h-1][0] and i[3] in speed_list[h-1][3]:
            pass
        else:
            id_index.append(h)
            #ave_list.append()
    id_index.append(len(speed_list))
    
    for h, i in enumerate(id_index[:-1]):
        total = 0.0
        #print i
        for j, k in enumerate(speed_list[id_index[h]:id_index[h+1]]):
            
            total += float(k[5])
        
        ave_list.append(k)
        ave_list[-1][5] = (total/(id_index[h+1]-id_index[h]))
        
        
    # minus the index as leng
    
    #i want [3] = future t
    #[5] = final prediction
    #dict look up with
    
    return ave_list

def get_speed(filename):
    #get the speed data and return list
        
    with open(filename, 'r') as fp:
        next(fp)
        reader = csv.reader(fp)
        speed_list = list(reader)

    fp.close()
    return speed_list

# get individual instances(minutes) of <60% of reference speed
def bottleneck_check(ave_list):

    #[4] is reference speed
    #[3] is average speed
    #[2] is speed
    
    #2 is bottleneck
    bottleneck = []

    for i in ave_list:
        
        if float(i[5]) >= 1.5:
            bottleneck.append(i)
    
    #sort by date then by tmc code for parsing
    bottleneck.sort(key = lambda x: x[3])
    bottleneck.sort(key = lambda x: x[0])
    return bottleneck

#do everything
def bottleneck_queue(filename):
    #get speed list
    print"Reading File:", filename

    speed_list = get_speed(filename)
    print "File Read"
    
    
    tmc_dict = get_tmc_dict()

    print "Prediction Output Length:", len(speed_list)
    ave_list = average_index(speed_list, tmc_dict)
    #for i in ave_list:
        #print i
    #get list of all individual bottleneck rows
    print "Length after averaging boxid indices:", len(ave_list)
    bottleneck_list = bottleneck_check(ave_list)
    #for i in bottleneck_list:
        #print i
    print "Length of entries matching bottleneck conditions:", len(bottleneck_list)


    bottleneck_index = []

    #loop through list of individual bottlenecks and get indexes of whenever tmc or date changes or mor than 9 minutes have passed
    for h,i in enumerate(bottleneck_list):
        if i[0] in bottleneck_list[h-1][0] and i[3][:10] in bottleneck_list[h-1][3][:10] and (int(i[3][8:10])*60*24) + (int(i[3][11:13])*60 + int(i[3][14:16])) < \
        (int(bottleneck_list[h-1][3][8:10])*60*24) + (int(bottleneck_list[h-1][3][11:13])*60 + int(bottleneck_list[h-1][3][14:16]) + 11):#  
            continue
        else:
            bottleneck_index.append(h)
        

    bottleneck_index.append(len(bottleneck_list))
    bottleneck_tmc = []
    bottleneck_start_time = []
    bottleneck_end_time = []
    duration = []

    for i, j in enumerate(bottleneck_index[:-1]):
        start_num = int(bottleneck_list[j][3][8:10])*24*60 + int(bottleneck_list[j][3][11:13])*60 + int(bottleneck_list[j][3][14:16])
        end_num = int(bottleneck_list[bottleneck_index[i+1]-1][3][8:10])*24*60 + int(bottleneck_list[bottleneck_index[i+1]-1][3][11:13])*60 + int(bottleneck_list[bottleneck_index[i+1]-1][3][14:16])
        if end_num - start_num >= 5:
            start_time = bottleneck_list[j][3]
            bottleneck_start_time.append(start_time)
            bottleneck_tmc.append(bottleneck_list[j][0])
            end_time = bottleneck_list[bottleneck_index[i+1]-1][3]
            bottleneck_end_time.append(end_time)
            
            
            duration.append((int(end_time[11:13])*60 + 1+ int(end_time[14:16]) - \
                             (int(start_time[11:13])*60 + int(start_time[14:16]))))
                        
    #create a tuple of the tmc, start, end and duration of the individual road segment bottlenecks
    queue_list=zip(bottleneck_tmc, bottleneck_start_time, bottleneck_end_time, duration)
    #sort by start time
    queue_list.sort(key = lambda x: x[1])
    
    #get the adjacents from tmc_adjacent_check.py
    adjacent_dict = get_adjacent()
    print "Getting Adjacents"
    # check for adjacents and change end times for individual segments that are adjacent to each other.
    match_index= check_adjacents(queue_list, adjacent_dict)
    print "Getting Matches"
    
    final_queue = group_adjacents(queue_list, match_index)
    print "Grouping Adjacent Matched Bottlenecks"
    length_dict = get_length()
    
    #not done
    print "Culling by Bottleneck Queue Length"
    final = cull_distance(final_queue, length_dict)

    print filename, "Done"
    for i in final:
        print i
        
    
    return final

#cull bottleneck queues based on length.  remove if total queue length is not > 0.3 miles
def cull_distance(final_queue,length_dict):
    final_list = []
    
    for i in final_queue:
        #print len(i)
        distance = 0
        
        try:
            distance += float(length_dict[i[0]])
        except KeyError:
            dup_list = []
            for j in i:
                if j not in dup_list:
                    dup_list.append(j)
                
            for k in dup_list:  
                distance += float(length_dict[k[0]])

        #print distance
        if distance > 0.3:
            final_list.append(i)
    
    b=0
    for i in final_list:
        for j in i:
            b+=1
    print "Total Segments after Distance Culling:", b
    return final_list

def group_adjacents(queue_list, match_index):
    print "Individual Bottleneck Length: ", len(queue_list)
    group_queue = []#  list of matches append to ?

    #loop through list of individual bottlenecks and the list of matches and assemble ones that have overlapping times
    for i, j in enumerate(queue_list):
        
        single_queue = []
        single_queue.append(j)

        for m in single_queue:

            for k, l in enumerate(match_index):
                try:
                    #print m[0], l[1]
                    if m[0] == l[1]:
                        #check if times overlap
                        start, end = convert_time(m[1], m[2])
                        start_c, end_c = convert_time(queue_list[l[2]][1], queue_list[l[2]][2])
                        
                        #if times overlap append to single_queue and set appended bottleneck to None
                        if start_c >= start and start_c <= end and queue_list[l[2]][1][:10] == m[1][:10]:
                            if queue_list[l[2]] not in single_queue:
                                single_queue.append(queue_list[l[2]])
                            queue_list[l[2]] = None
                    
                except TypeError:
                    pass
  
        #print single_queue
        try:
            single_queue.sort(key = lambda x: x[1])
        except TypeError:
            pass
        group_queue.append(single_queue)
            #print i, single_queue
    final_queue = []
    
    # remove rows with None
    for i in group_queue:
        #print i[0]
        if i[0]== None:
            pass

        else:

            final_queue.append(i)

    #sanity check
    b=0
    
    for i in final_queue:
        for j in i:
            b+=1
    print "Number of Individual Bottlenecks in Final Queue:", b

    return final_queue


#convert time to minutes total since midnight
def convert_time(start,end):
    
    start = (int(start[8:10])*60*24) + (int(start[11:13])*60) + int(start[14:16])
    end = (int(end[8:10])*60*24) + (int(end[11:13])*60 + int(end[14:16]))
    
    return start, end

        
        
# return list of index of all adjacents and from queue_list removes duplicates
def check_adjacents(queue_list, adjacent_dict):
    
    match_index = []#[None]*len(queue_list)
    for queue_index, query in enumerate(queue_list):
        #print query[0]
        for dict_index, adjacent in enumerate(adjacent_dict[query[0]]):

            for matched_index, matched_query in enumerate(queue_list):
                if adjacent == matched_query[0]:
                    match_index.append([queue_index, query[0], matched_index, matched_query[0]])
 

    #remove duplicates
    for i, j in reversed(list(enumerate(match_index))):
        #duplicate = False
        for k, l in enumerate(match_index):
            if j[0] == l[2] and j[2] == l[0]:
                match_index.pop(i)
                #duplicate = True
        #if duplicate == False:
            

            
    return match_index#, old_match_index


#make dict with tmc queries as key, and the matching adjacents as values
def get_adjacent():

    with open(sys.argv[2], "r") as fp:
        reader = csv.reader(fp, delimiter = "\t")
        tmc_adjacent_dict = {rows[0]:list(rows[1:]) for rows in reader}

    fp.close()
    return tmc_adjacent_dict

#make dict with tmc queries as key, and the length of the road segment as values
def get_length():
    
    with open(sys.argv[3], 'r') as fp:
        next(fp)
        reader = csv.reader(fp)
        tmc_length_dict = {rows[0]:float(rows[11]) for rows in reader}

    fp.close()
    return tmc_length_dict

def get_tmc_dict():
    
    with open(sys.argv[4], 'r') as fp:
        next(fp)
        reader = csv.reader(fp)
        tmc_dict = {rows[5]:rows[0] for rows in reader}
        
    fp.close()
    
    return tmc_dict

if __name__ == '__main__':
    main()
