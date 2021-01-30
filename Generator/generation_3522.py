#!/usr/bin/python3
# -*- coding: utf-8 -*-
import simulationDB
import numpy as np

#np.random.seed(2574)
np.random.seed(2345)

for dbi in range(10):
	sim = simulationDB.OpenSimulation(nomencl="/home/tguyet/Progs/medtrajectory_datagen/Generator/snds_nomenclature.db", datarep="/home/tguyet/Progs/medtrajectory_datagen/datarep")
	sim.nb_patients=10000 #all
	sim.nb_physicians=1000 #all
	sim.dpts=[35,22,56,29]

	sim.verbose=True

	sim.run()
	dbgen = simulationDB.simDB()
	dbgen.output_db_name="snds_Brittany_db%d.db"%dbi
	dbgen.generate(sim, rootschemas="../external/schema-snds-master/schemas")


	"""
	from importlib import reload
	reload(simulationDB)
	dbgen = simulationDB.simDB()
	dbgen.output_db_name="snds_2235.db"
	dbgen.generate(sim, rootschemas="../external/schema-snds-master/schemas")
	"""
