#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlalchemy as sa
import os
from tableschema import Table
import glob
import sqlalchemy as sa
import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from datetime import date, datetime


rootdir= os.getcwd()

rootsnomencl="../external/schema-snds-master/nomenclatures"

print("Create database: '" + os.path.join(rootdir,'snds_nomenclature.db') + "'")
# Create SQL database
#db = sa.create_engine('sqlite://') #save in memory
db = sa.create_engine('sqlite:///'+os.path.join(rootdir,'snds.db'))

print("### Chargement détaillé des nomenclatures (avec valeurs) ###")
#Load each of the nomenclature 
print("=> ORAVAL")
for fjson in glob.glob(rootsnomencl+"/ORAVAL/*.json"):
	fcsv = fjson[:-4]+"csv"
	table=Table(fcsv, schema=fjson)
	print("\t* load table '" +str(table.schema.descriptor['name'])+"': "+str(table.schema.descriptor.setdefault('title', "---")) )
	table.save(table.schema.descriptor['name'], storage='sql', engine=db)

print("=> DREES")
for fjson in glob.glob(rootsnomencl+"/DREES/*.json"):
	fcsv = fjson[:-4]+"csv"
	table=Table(fcsv, schema=fjson)
	print("\t* load table '" +str(table.schema.descriptor['name'])+"': "+str(table.schema.descriptor.setdefault('title', "---")) )
	table.save(table.schema.descriptor['name'], storage='sql', engine=db)

print("=> ORAREF")
for fjson in glob.glob(rootsnomencl+"/ORAREF/*.json"):
	fcsv = fjson[:-4]+"csv"
	table=Table(fcsv, schema=fjson)
	print("\t* load table '" +str(table.schema.descriptor['name'])+"': "+str(table.schema.descriptor.setdefault('title', "---")) )
	table.save(table.schema.descriptor['name'], storage='sql', engine=db)

print("=> ATIH")
for fjson in glob.glob(rootsnomencl+"/ATIH/*.json"):
	fcsv = fjson[:-4]+"csv"
	table=Table(fcsv, schema=fjson)
	print("\t* load table '" +str(table.schema.descriptor['name'])+"': "+str(table.schema.descriptor.setdefault('title', "---")) )
	table.save(table.schema.descriptor['name'], storage='sql', engine=db)

### Load the database schema ###
## -> the order in which the different parts of the schema are loader is very important (foreign key dependencies)

