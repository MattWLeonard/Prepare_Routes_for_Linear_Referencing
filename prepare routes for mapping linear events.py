# Prepare copy of ROUTE_MILEPOST for mapping route events:
# - Split routes where TRACKTYPE varies in TRACKSEGMENT;
# - Join TRACKTYPE from TRACKSEGMENT
# - Remove routes with bad/invalid measures

################################################################################

import arcpy as a
from arcpy import env
import sys
import datetime


# access current mxd
a.mapping.MapDocument('CURRENT')   # use if running within ArcMap
mxd = a.mapping.MapDocument('CURRENT')   # use if running within ArcMap


# set input/output location (to be workspace)
gdb = "C:/MWLlocal/TSImapping_local/TSI_2023/TSI_2023.gdb"

# set workspace
a.env.workspace = gdb


# today's date
today = datetime.datetime.now()
year = str(today.year)
if len(str(today.month)) == 2:
	month = str(today.month)
else:
	month = "0" + str(today.month)
if len(str(today.day)) == 2:
	day = str(today.day)
else:
	day = "0" + str(today.day)
datesuffix = year + month + day


################################################################################

# input data:
#############

# ITGIS_TRACKSEGMENT (local copy updated)
tracksegment = "C:/MWLlocal/local_copies_reference_data/local_copies.gdb/ITGIS_TRACKSEGMENT_local"

# ITGIS_ROUTE_MILEPOST (local copy updated)
routemp = "C:/MWLlocal/local_copies_reference_data/local_copies.gdb/ITGIS_ROUTE_MILEPOST_local"


### input variables:
### N/A


# output data:
##############

# exported tracksegment endpoints to be flagged
ends = "TRACKSEGMENT_ENDPTS_" + datesuffix  

# export of flagged tracksegment endpoints to be used for splitting routes
splitends = "LRS_SPLIT_PTS_" + datesuffix  	

# routes split for tracktype, to be used for mapping events (will get tracktype joined in next steps)
routes_forTracktype = "ROUTE_MILEPOST_SplitForTracktype_" + datesuffix   

# route midpoints output, to get tracktype joined next
route_mids = "ROUTE_MIDS_" + datesuffix  

# route mids w/tracktype joined,  to be used to join tracktype to routes
routemids_wTracktype = "ROUTE_MIDS_wTRACKTYPE_" + datesuffix    

# final output routes, ready for mapping events!
finalroutes = "ROUTES_wTRACKTYPE_CLEAN_" + datesuffix  

# duplicate routes with MLN_MAINT (to be appended back to finalroutes)
maintdivroutes = "ROUTES_DUP_MAINTDIV_" + datesuffix

################################################################################


# in TRACKSEGMENT: identify features which share endpoints and share LRS_ROUTEID
#    but TRACKTYPE is different:

def TracksegmentEndsPrep():
	# export endpoints from TRACKSEGMENT
	a.FeatureVerticesToPoints_management(tracksegment,ends,"BOTH_ENDS")
													  #^output

	# in endpoints, add and calc coords combo field
	a.AddField_management(ends,"COORDS","TEXT","","",30)
	
	with a.da.UpdateCursor(ends, ["COORDS","SHAPE@XY"]) as cursor:  # works
		for row in cursor:
			row[0] = str(round(row[1][0],6)) + " " + str(round(row[1][1],6))
			cursor.updateRow(row)
	del cursor
	
	# in endpoints, add field for flagging points where routes should be split
	a.AddField_management(ends,"LRS_ROUTE_SPLIT","TEXT","","",1)

	print "tracksegment endpoints prep done."
	

def TracksegmentEndFlagging():	# works!
	# in endpoints, use cursor to flag points where coords match, LRS_ROUTEID matches,
	#    but TRACKTYPE doesn't match. flag only one point at each of these locations.
	#    (these are points where routes should be split to enable joining 1:1 TRACKTYPE field)
	cursorfields = 	["COORDS","LRS_ROUTEID","TRACKTYPE","LRS_ROUTE_SPLIT"]
	sql = (None, "ORDER BY COORDS ASC, LRS_ROUTEID ASC, TRACKTYPE ASC")	
	with a.da.UpdateCursor(ends, cursorfields, sql_clause=sql) as cursor:
		count = 0
		for row in cursor:
			if count == 0:  # first point, establish starting variables
				coords_prev = row[0]
				lrs_prev = row[1]
				tt_prev = row[2]
				count += 1
			else:  # all subsequent points
				coords = row[0]
				lrs = row[1]
				tt = row[2]
				
				if coords == coords_prev:
					if lrs == lrs_prev:
						if tt == tt_prev:
							pass
						else:  # flag this point
							row[3] = "Y"
							cursor.updateRow(row)
					else:
						pass
				else:
					pass
				
				# redefine prev values
				coords_prev = row[0]
				lrs_prev = row[1]
				tt_prev = row[2]
	del cursor
	
	# export flagged endpoints to new FC
	whereclause = "LRS_ROUTE_SPLIT = 'Y'"
	a.FeatureClassToFeatureClass_conversion(ends,gdb,splitends,whereclause)
 													 #^output
	print "tracksegment endpoints flagging done."
	

################################################################################

# split ROUTE_MILEPOST at those designated split points
def SplitRoutes():
	a.SplitLineAtPoint_management(routemp,splitends,routes_forTracktype)
													#^output
	print "route splitting at endpoints done."
	

# add TRACKTYPE to route mids:
def TracktypeToRouteMids():
	# export midpoints from now split ROUTE_MILEPOST
	a.FeatureVerticesToPoints_management(routes_forTracktype,route_mids,"MID")
															 #^output
	
	# spatial join only TRACKTYPE field from TRACKSEGMENT to route mids
	# use field mappings to join only selected field(s)
	fieldmappings =	a.FieldMappings()
	fieldmappings.addTable(route_mids)  				# keep all fields from targetfc
	for f in a.ListFields(tracksegment):
		if f.name == "TRACKTYPE":
			fm = a.FieldMap() # create empty field map object for one field
			fm.addInputField(tracksegment,f.name) # populate field map with input field
			fieldmappings.addFieldMap(fm) # add field map to field mappings object
		else:
			pass
	match = 	"CLOSEST_GEODESIC"
	a.SpatialJoin_analysis(route_mids,tracksegment,routemids_wTracktype,"","KEEP_COMMON",fieldmappings,match,"1 Feet")
												   #^output
	print "tracktype join to route mids done."
	
	
# join TRACKTYPE field from route mids to ROUTE_MILEPOST
def TracktypeToRoutes():
	# add tracktype field to routes (which don't actually have tracktype yet despite the name)
	a.AddField_management(routes_forTracktype,"TRACKTYPE","TEXT","","",30)
	
	# build dictionary of values in join field
	join_dict = {}
	cursorfields =	["ORIG_FID","TRACKTYPE"]
	with a.da.SearchCursor(routemids_wTracktype, cursorfields) as cursor:
		for row in cursor:	
			join_dict[row[0]] = row[1]
	del cursor	

	# cursor to populate join fields in target table from dictionary
	cursorfields =	["OBJECTID","TRACKTYPE"]
	with a.da.UpdateCursor(routes_forTracktype, cursorfields) as cursor:
		for row in cursor:
			if row[0] in join_dict:		# account for target records with no match
				row[1] = join_dict[row[0]]
				cursor.updateRow(row)
	del cursor
	del join_dict

	print "tracktype join from route mids to routes done."


# add and populate field MLN_TRACKTYPE
def MLN_TRACKTYPE():
	a.AddField_management(routes_forTracktype,"MLN_TRACKTYPE","TEXT","","",30)
	
	cursorfields =	["MASTERLINENAME","TRACKTYPE","MLN_TRACKTYPE"]
	whereclause = "TRACKTYPE IS NOT NULL"
	with a.da.UpdateCursor(routes_forTracktype, cursorfields, whereclause) as cursor:
		for row in cursor:
			row[2] = row[0] + "_" + row[1]
			cursor.updateRow(row)
	del cursor	
	

################################################################################

# filter out features with bad measures in ROUTE_MILEPOST: 		<<<<<<<<<<<<<<<< (btw SAVE THIS PART AS STANDALONE SCRIPT)

def CleanupRoutesPart1(): # works only with PYTHON_9.3!
	# revise begin and end milepost values b/c some features have been split
	# now BEGINMILEPOST and ENDMILEPOST values will match start/end M values
	#  (couldn''t get cursor method to work here)
	a.CalculateField_management(routes_forTracktype,"BEGINMILEPOST","!Shape!.firstPoint.M","PYTHON_9.3")	# works.
	a.CalculateField_management(routes_forTracktype,"ENDMILEPOST","!Shape!.lastPoint.M","PYTHON_9.3")

	print "BEGINMILEPOST & ENDMILEPOST recalc done."
	

def CleanupRoutesPart2():

	# add fields for min/max M values (not always same as start/end, though they should be), and populate
	a.AddField_management(routes_forTracktype,"M_MIN","DOUBLE")
	a.AddField_management(routes_forTracktype,"M_MAX","DOUBLE")
	
	# add field for phys length, & M length
	a.AddField_management(routes_forTracktype,"LENGTH_MI","DOUBLE")
	a.AddField_management(routes_forTracktype,"LENGTH_M","DOUBLE")
	
	# add EXCLUDE field
	a.AddField_management(routes_forTracktype,"EXCLUDE","TEXT","","",20)
	
	print "adding fields M_MIN M_MAX LENGTH_MI LENGTH_M EXCLUDE done."
	

def CleanupRoutesPart3():
	# populate fields for min M, max M
	a.CalculateField_management(routes_forTracktype,"M_MIN","!Shape!.extent.MMin","PYTHON_9.3")
	a.CalculateField_management(routes_forTracktype,"M_MAX","!Shape!.extent.MMax","PYTHON_9.3")
		
	# populate fields for phys length, M length
	cursorfields = 	["BEGINMILEPOST","ENDMILEPOST","LENGTH_MI","LENGTH_M","SHAPE@"]
	with a.da.UpdateCursor(routes_forTracktype, cursorfields) as cursor:
		for row in cursor:
			# populate phys length
			row[2] = row[4].getLength("GEODESIC","MILES")			# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< CHECK THIS!
			# populate M length
			row[3] = row[1]-row[0]

			cursor.updateRow(row)
	del cursor
	
	print "populating MMin MMax LENGTH_MI LENGTH_M done."
	

def CleanupRoutesPart4():
		
	# flag/delete features with measures above/below start/end M values
	cursorfields = 	["BEGINMILEPOST","ENDMILEPOST","M_MIN","M_MAX","EXCLUDE"]
	whereclause = 	"(M_MIN < BEGINMILEPOST) OR (M_MAX > ENDMILEPOST)"  # CONDITION FOR EXCLUSION
	with a.da.UpdateCursor(routes_forTracktype, cursorfields, whereclause) as cursor:
		for row in cursor:
			row[4] = "hi/low measures"  # flag to exclude
			cursor.updateRow(row)
	del cursor
	
	print "flagging measures above/below start/end done."
	

	# flag/delete features with backwards measures
	cursorfields = 	["BEGINMILEPOST","ENDMILEPOST","EXCLUDE"]
	whereclause = 	"ENDMILEPOST <= BEGINMILEPOST"  # CONDITION FOR EXCLUSION
	with a.da.UpdateCursor(routes_forTracktype, cursorfields, whereclause) as cursor:
		for row in cursor:
			row[2] = "backwards measures"  # flag to exclude
			cursor.updateRow(row)
	del cursor
	
	print "flagging backwards measures done."
	

def CleanupRoutesPart5():
		
	# flag features with big difference between LENGTH_MI & LENGTH_M (in 3 phases)
	# flag features with measures in feet not miles
	
	cursorfields = 	["LENGTH_MI","LENGTH_M","EXCLUDE"]
	
	whereclause = "(LENGTH_MI <= 0.5)  AND  ( ((LENGTH_M-LENGTH_MI) < -1.5) OR ((LENGTH_M-LENGTH_MI) > 1.5) )"  # diff no more than 1.5
	with a.da.UpdateCursor(routes_forTracktype, cursorfields, whereclause) as cursor:
		for row in cursor:
			row[2] = "bad M calibration"  # flag to exclude
			cursor.updateRow(row)
	del cursor
	

	whereclause = "( (0.5 < LENGTH_MI) AND (LENGTH_MI <= 20) ) AND ( ((LENGTH_M-LENGTH_MI) < -5  ) OR ((LENGTH_M-LENGTH_MI) > 5) )"  # diff no more than 5
	with a.da.UpdateCursor(routes_forTracktype, cursorfields, whereclause) as cursor:
		for row in cursor:
			row[2] = "bad M calibration"  # flag to exclude
			cursor.updateRow(row)
	del cursor
	

	whereclause = "(LENGTH_MI > 20)  AND  ( ((LENGTH_M-LENGTH_MI) < -15 ) OR ((LENGTH_M-LENGTH_MI) > 15) )"  # diff no more than 15
	with a.da.UpdateCursor(routes_forTracktype, cursorfields, whereclause) as cursor:
		for row in cursor:
			row[2] = "bad M calibration"  # flag to exclude
			cursor.updateRow(row)
	del cursor


	whereclause = "(M_MIN > 900) OR (M_MAX > 900)"  # any measure > 900 is wrong
	with a.da.UpdateCursor(routes_forTracktype, cursorfields, whereclause) as cursor:
		for row in cursor:
			row[2] = "bad M calibration"  # flag to exclude
			cursor.updateRow(row)
	del cursor

	
	print "flagging bad M calibration done."


################################################################################

def ExportRoutes():
	# export routes without flagged (excluded) features
	whereclause = "(EXCLUDE IS NULL) AND (MLN_TRACKTYPE IS NOT NULL)"
	a.FeatureClassToFeatureClass_conversion(routes_forTracktype,gdb,finalroutes,whereclause)
													 #^output
	print "final route export done."



################################################################################

# Add duplicate (alternate) routes where maintenance division differs:

def MaintDivPart1():
	# add field for MLN_MAINT
	a.AddField_management(finalroutes,"MLN_MAINT","TEXT","","",6)
	
	# add field for NOTE
	a.AddField_management(finalroutes,"NOTE","TEXT","","",100)
	
	# PREPARE DICTIONARY OF OP-MAINT CONFLICT RANGES (one range per MLN)		# <<< DOES NOT PERSIST outside the function.
	#  ( format like  MLN op : [MLN mt, LMP, HMP, div op, div mt, pre, suf] )
	op_maint_dict = {
		'__04__' : ['__08__',650,669.83,'04','08','',''],
		'__08A_' : ['__04A_',226.72,235,'08','04','','A'],
		'__08AS' : ['__62AS',142.33,142.36,'08','62','','AS'],
		'__08I_' : ['__04I_',1.2,14.75,'08','04','','I'],
		'__62__' : ['__04__',347.15,362.7,'62','04','',''],
		'__62T_' : ['__08T_',0,40,'62','08','','T'],
		'__62TB' : ['__08TB',0,25.5,'62','08','','TB'],
		'_B72__' : ['_B76__',172.05,197.07,'72','76','B',''],
		'_C04__' : ['_C08__',270,361.4,'04','08','C',''],
		'_M04__' : ['_M08__',287,291,'04','08','M',''],
		'_O04__' : ['_O62__',3.9,26,'04','62','O',''],
		'_P04__' : ['_P08__',291,321,'04','08','P',''],
		'_S62__' : ['_S76__',4.4,13.6,'62','76','S',''],
		'AI72__' : ['AI76__',0,1.44,'72','76','AI',''],
		'AM62__' : ['AM94__',132.1,137.57,'62','94','AM',''],
		'AR04__' : ['AR08__',1.2,12.01,'04','08','AR',''],
		'CB72__' : ['CB76__',0,6.7,'72','76','CB',''],
		'CD72__' : ['CD76__',181.16,193.91,'72','76','CD',''],
		'CF94__' : ['CF76__',169,178.94,'94','76','CF',''],
		'CJ62__' : ['CJ94__',134.4,150.67,'62','94','CJ',''],
		'CX62__' : ['CX08__',0,1.3,'62','08','CX',''],
		'GO04__' : ['GO08__',2.6,12,'04','08','GO',''],
		'GZ72__' : ['GZ76__',484,493.32,'72','76','GZ',''],
		'IK94__' : ['IK76__',124.8,127.2,'94','76','IK',''],
		'IX72__' : ['IX76__',0,0.47,'72','76','IX',''],
		'KM62__' : ['KM94__',0.39,8.27,'62','94','KM',''],
		'MP94__' : ['MP76__',0.5,110.97,'94','76','MP',''],
		'OJ72__' : ['OJ76__',25.5,27.4,'72','76','OJ',''],
		'PN72__' : ['PN76__',0,1.8,'72','76','PN',''],
		'QZ62__' : ['QZ94__',0,3.3,'62','94','QZ',''],
		'RD72__' : ['RD76__',86,123.14,'72','76','RD',''],
		'RK94__' : ['RK76__',154.5,162.03,'94','76','RK',''],
		'RR62__' : ['RR94__',0,7,'62','94','RR',''],
		'SW04__' : ['SW08__',291,292.8,'04','08','SW',''],
		'WA04__' : ['WA08__',0,7,'04','08','WA',''],
		'ZC72__' : ['ZC76__',0,2.4,'72','76','ZC',''],
		'ZF72__' : ['ZF72__',0.9,1.2,'72','72','ZF',''],
		}

	# If route all or partially overlaps one of the conflict ranges,
	#  populate MLN_MAINT from dictionary, and populate MLN_TRACKTYPE_MAINT.
	cursorfields = 	["MASTERLINENAME","BEGINMILEPOST","ENDMILEPOST","MLN_MAINT"]
	with a.da.UpdateCursor(finalroutes, cursorfields) as cursor:
		for row in cursor:
			mln = row[0]
			lmp = row[1]
##			print "lmp = " + str(lmp)  # for testing
			hmp = row[2]
##			print "hmp = " + str(hmp)  # for testing
			if mln in op_maint_dict:
				mln_maint = op_maint_dict[mln][0]
				lmp_range = op_maint_dict[mln][1]
##				print "lmp_range = " + str(lmp_range)  # for testing
				hmp_range = op_maint_dict[mln][2]
##				print "hmp_range = " + str(hmp_range)  # for testing
				if lmp < hmp_range:
					if hmp > lmp_range:
						row[3] = mln_maint
						cursor.updateRow(row)
				else:
					pass
			else:
				pass
	del cursor


def MaintDivPart2():
	# EXPORT routes with MLN_MAINT now populated as duplicates for maint div conflicts
	whereclause = "MLN_MAINT IS NOT NULL"
	a.FeatureClassToFeatureClass_conversion(finalroutes,gdb,maintdivroutes,whereclause)
														#^output
	
	# in exported duplicates, recalc MLN_TRACKTYPE to use MLN_MAINT
	#  and calc NOTE = "duplicate alternate feature for maintenance division"
	cursorfields = 	["MLN_MAINT","TRACKTYPE","MLN_TRACKTYPE","NOTE"]
	with a.da.UpdateCursor(maintdivroutes, cursorfields) as cursor:
		for row in cursor:
			row[2] = row[0] + "_" + row[1]
			row[3] = "duplicate alternate feature for maintenance division"
			cursor.updateRow(row)
	del cursor
	
	# append duplicates back to finalroutes
	#  (now finalroutes can be used to map all events at once, including those with maint div conflicts)
	a.Append_management(maintdivroutes,finalroutes,"NO_TEST")


################################################################################

# RUN FUNCTIONS:
TracksegmentEndsPrep()		# ~1 min.	ok
TracksegmentEndFlagging()	# ~1 min.	ok
SplitRoutes()				# ~1-2 min.	ok
TracktypeToRouteMids()		# ~5-6 min.	ok
TracktypeToRoutes()			# <1 min.	ok
MLN_TRACKTYPE()				# <1 min.	ok

CleanupRoutesPart1()		# ~1 min.	ok
CleanupRoutesPart2()		# <1 min.	ok
CleanupRoutesPart3()		# ~2 min.	ok
CleanupRoutesPart4()		# <1 min.	ok
CleanupRoutesPart5()		# <1 min.	ok
ExportRoutes()				# <1 min.	ok

MaintDivPart1()				# <1 min.	ok
MaintDivPart2()				# <1 min.	ok
