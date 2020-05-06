# -*- coding: utf-8 -*-


from data_factory import FactoryContext, PatientFactory, PhysicianFactory, PharmacyFactory, DrugsDeliveryFactory
import os


class simulation:
	def __init__(self):
		self.nomencl_db = "/home/tguyet/Progs/SNDS/medtrajectory_datagen/Generator/snds_nomenclature.db"
		self.context = FactoryContext(nomenclatures=self.nomencl_db)
		self.pharms = []
		self.GPs = []
		self.specialists = []
		self.patients = []

	def run(self):
		pfactory=PharmacyFactory(self.context)
		self.pharms = pfactory.generate(10)
		
		pfactory=PhysicianFactory(self.context)
		self.GPs = pfactory.generateGP(5)
		self.specialists = pfactory.generateSpecialists(10)
		
		pfactory=PatientFactory(self.context, self.GPs)
		self.patients = pfactory.generate(10)
		
		drugfact=DrugsDeliveryFactory(self.context, self.pharms)
		for p in self.patients:
			drugfact.generate(p,50)
	


if __name__ == "__main__":
	sim = simulation()
	sim.run()
	
	for p in sim.patients:
		for dd in p.drugdeliveries:
			print(dd)
		print("========")
