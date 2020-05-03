#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  3 11:07:38 2020

@author: tguyet
"""

from data_factory import FactoryContext, PatientFactory
import os

os.chdir("/home/tguyet/Progs/SNDS/medtrajectory_datagen/Generator")


context=FactoryContext()

pfactory=PatientFactory(context)

patients = pfactory.generate(10)

for p in patients:
	print(p)