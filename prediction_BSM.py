import sys
import csv
import argparse
import datetime
import time
import itertools
import pandas as pd
import networkx as nx
from itertools import chain
from collections import defaultdict
from mpi4py import MPI
import sqlite3

MIN_INDEX = 1
MAX_INDEX = 4

def count_common_edges(gx, gy):
	# calculate the smaller and larger graph
	gs = gx
	gl = gy
	if gx.number_of_edges() > gy.number_of_edges():
		gs = gy
		gl = gx
	# iterate over the edges of the smaller graph
	# accumulate the number of shared edges
	e = 0.0
	for s, t in gs.edges_iter():
		# boolean automatically converted to integer
		e += gl.has_edge(s, t)
	return e

def load_date_sets(path='DaySets'):
	with open(path, 'rb') as fh:
		training_dates = set()
		validation_dates = set()
		test_dates = set()
		for line in fh:
			row = line.strip().split('\t')
			day, type = row
			if type == 'Training':
				training_dates.add(day)
			if type == 'Validation':
				validation_dates.add(day)
			if type == 'Test':
				test_dates.add(day)
	return (training_dates, validation_dates, test_dates)

def load_network_from_date_query(curs, date):
	curs.execute('SELECT src, tar FROM edges WHERE date=?', [date])
	G = nx.Graph()
	for src, tar in curs:
		G.add_edge(src, tar)
	return G

def main(argv):
	comm = MPI.COMM_WORLD
	rank = comm.Get_rank()
	size = comm.Get_size()
	BOSS = 0
	if rank == 0:

		print "Starting by processing dates " + str(rank)
		# Connect to database to make day sets
		with open(argv[0], "wb") as out_f:
			out_f.write("boxid,direction,t,future_t,actual_bi, final_prediction, prediction_error\n")
		conn = sqlite3.connect('graphs_db.sqlite')
		curs = conn.cursor()
		# Load date sets
		training_dates, validation_dates, test_dates = load_date_sets(argv[1])
		print validation_dates
		# Calculate data hour sets
		training_date_hours = set()
		validation_date_hours = set()
		test_date_hours = set()
		curs.execute('SELECT DISTINCT(date) FROM edges;')
		for row in curs:
			date, hour = row[0].split('T')
			print date
			if date in training_dates:
				training_date_hours.add(row[0])
			if date in validation_dates:
				validation_date_hours.add(row[0])
			if date in test_dates:
				test_date_hours.add(row[0])

		training_date_hours = list(training_date_hours)
		validation_date_hours = list(validation_date_hours)
		test_date_hours = list(test_date_hours)

		print validation_date_hours

		chunks = [[] for _ in range(size)]
		for i, chunk in enumerate(validation_date_hours):
		    chunks[i % size].append(chunk)

		curs.close()
		conn.close()
	else:
	    validation_date_hours = None
	    training_date_hours = None
	    chunks = None
	training_date_hours = comm.bcast(training_date_hours, root=0)    
	validation_date_hours = comm.scatter(chunks, root=0)
	print str(rank) + ': ' + str(validation_date_hours)

	print "Calculating initscreen " + str(rank)
	conn = sqlite3.connect('graphs_db.sqlite')
	curs = conn.cursor()
	# Initial screen calculation
	initial_screen = {}
	THRESH_MU = 1.0 
	training_date_data = {}
	for y in training_date_hours:
		# Load network from database
		gy = load_network_from_date_query(curs, y)
		try:
			mu_y = sum(nx.average_neighbor_degree(gy).values()) / float(len(gy))
			training_date_data[y] = mu_y
		except ZeroDivisionError:
			pass
	for x in validation_date_hours:
		# Load network from database
		gx = load_network_from_date_query(curs, x)
		initial_screen[x] = []
		for y in training_date_data:
			dx, hx = x.split('T')
			dy, hy = y.split('T')
			if hx == hy:
				# Compare
				try:
					mu_x = sum(nx.average_neighbor_degree(gx).values()) / float(len(gx))
					mu_y = training_date_data[y]
					mu = (abs(mu_x - mu_y) / float(mu_y))
					if mu <= THRESH_MU:
						initial_screen[x].append(y)
				except ZeroDivisionError:
					pass
	print initial_screen 
	print "Calculating matched " + str(rank)

	# validation and initial screen comparison
	matched = {}
	THRESH_E = 0.04 # Note: threshold should be .8, taking .04
	for x in validation_date_hours:
		# load network from database
		gx = load_network_from_date_query(curs, x)
		for y in initial_screen[x]:
			# load network from database
			gy = load_network_from_date_query(curs, y)
			try:
				e = count_common_edges(gx, gy)
				e /= gx.number_of_edges()
				if e >= THRESH_E:
					matched[(x, y)] = e
			except ZeroDivisionError:
				pass
	print matched
	print "Making vm " + str(rank)
	vm = defaultdict(list)
	for k, v in matched.iteritems():
		x, y = k
		xdate,xhour = x.split('T')
		ydate,yhour = y.split('T')
		xdate = datetime.datetime.strptime(xdate,"%Y-%m-%d")
		ydate = datetime.datetime.strptime(ydate,"%Y-%m-%d")
		xhour = datetime.time(int(xhour))
		yhour = datetime.time(int(yhour))
		x = datetime.datetime.combine(xdate,xhour)
		y = datetime.datetime.combine(ydate,yhour)
		vm[x].append((y,v))
	print vm
	
	# close database handles
	curs.close()
	conn.close()

	results = []

	print "Doing Algorithm version 1.1"

	conn = sqlite3.connect('counts_db.sqlite') # Assume this is database containing all data
	conn.row_factory = lambda curs, row: {col[0]: row[idx] for idx, col in enumerate(curs.description)}
	# Cursor for data at future times on validation day
	curs1 = conn.cursor()
	# Cursor for data at current times on validation day
	curs2 = conn.cursor()
	# Cursor for data at future times on training days
	curs3 = conn.cursor()
	# Cursor for data at current time on training days
	curs4 = conn.cursor()
	

	print "Iterating through validation_date_hours"
	for v in validation_date_hours:
		final_prediction = None
		#Sprint s
		x,t = v.split('T')
		# Get time t on validation day in correct format
		t = int(t)
		vday = datetime.datetime.strptime(x, "%Y-%m-%d")
		vtime = datetime.time(t)
		vdaytime = datetime.datetime.combine(vday,vtime)
		ttime = datetime.timedelta(hours = t)
		conversion_v = time.mktime(vday.timetuple())
		tstamp_v = time.mktime(vdaytime.timetuple())
		# Only try to predict where data exists in any t' (future data on validation day) corresponding to t
		curs1.execute('SELECT * FROM \'' + x + '\' WHERE dt BETWEEN ? AND ?;', (tstamp_v + 3600, tstamp_v + 7200))
		#curs1.execute('SELECT (dt-?) AS future_t, CAST((dt-?)/300 AS INTEGER) AS interval, * FROM \'' + x + '\' WHERE dt BETWEEN ? AND ?;', (conversion_v, conversion_v, tstamp_v + 3600, tstamp_v + 7200))
		for row1 in curs1:
			#print row1
			# Do not make any prediction if there is no future data
			# Define the unique information for each prediction
			#print row1['interval']
			boxid = row1['id']
			direction = row1['dir']
			actual_bi = row1['bi']
			future_t = (row1['dt'] - conversion_v) #time in seconds since midnight

			# Now try to find data at current time (or closest data within 1 hour) in that boxid and direction 
			# In order to sort data by 5 minutes, divide future times into 5 minute intervals by dividing by 300 seconds
			curs2.execute('SELECT * FROM \'' + x + '\' WHERE id=? AND dir=? AND dt BETWEEN ? AND ? ORDER BY dt DESC LIMIT 1;', (boxid,direction,tstamp_v - 3600, tstamp_v))
			v_curr = curs2.fetchone()		
			# If there is data at the current time on the validation day
			if v_curr != None: 
				v_current_bi = v_curr['bi']
				# If there are no matches for the validation day at the current time then predict that future index is current index
				if vdaytime not in vm:
					final_prediction = v_current_bi
				# If there are matches, look for data on these training days
				else:
					print "success"
					# Keep track of calculations for primary equation (using training day future and current data)
					sum_for_both_num = 0
					sum_for_both_den = 0
					# Keep track of calculations for equation that does not depend on current data (in case there is none)
					sum_for_fut_num = 0
					sum_for_fut_den = 0
					# Keep track of calculations for primary equation for alternative 2 (using training day future and current data)
					sum_both_box_num = 0
					sum_both_box_den = 0
					# Keep track of calculations for equation that does not depend on current data for alternative 2 (in case there is none)
					sum_fut_box_num = 0
					sum_fut_box_den = 0

					# Look through matches
					for tdaytime,ps in vm[vdaytime]:
						# Find datetime information for future and current time on training day
						tday = tdaytime - ttime
						conversion_t = time.mktime(tday.timetuple())
						tstamp_t = time.mktime(tdaytime.timetuple())
						y = tdaytime.strftime("%Y-%m-%d")
						# Try to find data at future time on training day (in that box and direction)
						curs3.execute('SELECT * FROM \'' + y + '\' WHERE id=? AND dir=? AND dt=? LIMIT 1;', (boxid,direction,tstamp_t))
						tr_fut = curs3.fetchone()
						# If this data exists, update the 'future' calculations (otherwise keep looping)
						if tr_fut != None:
							fut_bi = tr_fut['bi']
							sum_for_fut_num += (fut_bi*ps)
							sum_for_fut_den += ps
							# Try to find data at current time on training day (or within 1 hour):
							curs4.execute('SELECT * FROM \'' + y + '\' WHERE id=? AND dir=? AND dt BETWEEN ? AND ? ORDER BY dt DESC LIMIT 1;',(boxid,direction,tstamp_t - 3600,tstamp_t))
							tr_curr = curs4.fetchone()
							# If data also exists at current time, update 'both' calculations (otherwise keep looping)
							if tr_curr != None:
								curr_bi = tr_curr['bi']
								sum_for_both_num += ((fut_bi - curr_bi)*ps)
								sum_for_both_den += ps
					# if there was enough data, make appropriate calculation
					if sum_both_box_den != 0 or sum_for_both_den != 0:
						final_prediction = v_current_bi + ((sum_for_both_num+sum_both_box_num)/(sum_for_both_den+sum_both_box_den))
					elif sum_fut_box_den != 0 or sum_for_fut_den != 0: 
						final_prediction = (sum_for_fut_num+sum_fut_box_num)/(sum_for_fut_den+sum_fut_box_den)
					else:
						final_prediction = v_current_bi
						########################################################################################################################################################################
			# If there is no current data on the validation day try to make 6.4 calculation
			else:
				# try to do alt2, 9.6.1.7 (that is)
				# If there are no matched results, then do not make any prediction
				if vdaytime not in vm:
					#print "match empty"
					continue
				# Keep track of calculations necessary for equation
				print "success"
				sum_matched_num = 0
				sum_matched_den = 0
				sum_box_matched_num = 0
				sum_box_matched_den = 0
				# Look through matches
				for tdaytime,ps in vm[vdaytime]:
					# Find datetime information for future and current time on training day
					tday = tdaytime - ttime
					conversion_t = time.mktime(tday.timetuple())
					tstamp_t = time.mktime(tdaytime.timetuple())
					y = tdaytime.strftime("%Y-%m-%d")
					# Try to find data at future time on training day
					curs3.execute('SELECT * FROM \'' + y + '\' WHERE id=? AND dir=? AND dt=? LIMIT 1;', (boxid,direction,conversion_t+future_t))
					tr_fut = curs3.fetchone()
					# If it exists, update calculations (otherwise keep looping)
					if tr_fut != None:
						fut_bi = tr_fut['bi']
						sum_matched_num += (fut_bi*ps)
						sum_matched_den += ps
				# if there was enough data to make a calculation, make it!
				if sum_box_matched_den != 0 or sum_matched_den  != 0:
					final_prediction = (sum_matched_num+sum_box_matched_num)/(sum_matched_den+sum_box_matched_den)
					#############################################################################################################################################
			if final_prediction == None:
				print "fp None"
				continue
			# Make sure prediction is within bounds of bi
			if final_prediction < MIN_INDEX:
				final_prediction = MIN_INDEX
			if final_prediction > MAX_INDEX:
				final_prediction = MAX_INDEX
			# Calculate prediction error
			prediction_error = abs(final_prediction - actual_bi)/float(actual_bi)
			results.append([str(boxid),str(direction),str(datetime.datetime.fromtimestamp(tstamp_v)),str(datetime.datetime.fromtimestamp(future_t + conversion_v)),str(actual_bi), str(final_prediction), str(prediction_error)])
	# Close all curs and connection
	curs1.close()
	curs2.close()
	curs3.close()
	curs4.close()
	conn.close()

	results_list = comm.gather(results, root=0)
	
	if rank == BOSS:
		with open(argv[0], "a") as out_f:
			for results in results_list:
				for result in results:
					out_f.write("%s,%s,%s,%s,%s,%s,%s\n" % (result[0],result[1],result[2],result[3],result[4],result[5],result[6]))

	return 0

if __name__ == '__main__':
	sys.exit(main(sys.argv))


