#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  3 11:07:38 2020

@author: tguyet
"""

from data_factory import FactoryContext, PatientFactory, PhysicianFactory, PharmacyFactory, DrugsDeliveryFactory
import os

os.chdir("/home/tguyet/Progs/medtrajectory_datagen/Generator")


context=FactoryContext()

pfactory=PharmacyFactory(context)
pharms = pfactory.generate(10)
for p in pharms:
	print(p)

pfactory=PhysicianFactory(context)
GPs = pfactory.generateGP(5)
specialists = pfactory.generateSpecialists(10)
for p in GPs+specialists:
	print(p)

#pfactory=PatientFactory(context, [p.id for p in GPs])
pfactory=PatientFactory(context, GPs)
patients = pfactory.generate(10)
for p in patients:
	print(p)

drugfact=DrugsDeliveryFactory(context,pharms)
for p in patients:
	drugfact.generate(p,50)
	for dd in p.drugdeliveries:
		print(dd)