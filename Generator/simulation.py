# -*- coding: utf-8 -*-
"""
Simulation class, it organizes the simulation processes.

@author: Thomas Guyet
@date: 08/2020
"""

from data_factory import FactoryContext, PatientFactory, PhysicianFactory, PharmacyFactory, DrugsDeliveryFactory, EtablissementFactory, VisitFactory, ActFactory, ShortStayFactory

class simulation:
    def __init__(self, nomencl="snds_nomenclature.db"):
        self.nomencl_db = nomencl
        self.context = FactoryContext(nomenclatures=self.nomencl_db)
        self.pharms = []
        self.GPs = []
        self.specialists = []
        self.patients = []
        self.etablissements = []

    def run(self):
        pfactory=PharmacyFactory(self.context)
        self.pharms = pfactory.generate(10)
        
        #un unique établissement
        self.etablissements.append( EtablissementFactory(self.context).generate() )
        
        pfactory=PhysicianFactory(self.context)
        self.GPs = pfactory.generateGP(5)
        self.specialists = pfactory.generateSpecialists(10)
        
        pfactory=PatientFactory(self.context, self.GPs)
        self.patients = pfactory.generate(10)
        
        drugfact=DrugsDeliveryFactory(self.context, self.pharms)
        for p in self.patients:
            drugfact.generate(p,50)

        visitfact=VisitFactory(self.context, self.specialists)
        for p in self.patients:
            visitfact.generate(p,30)
            
        actfact=ActFactory(self.context, self.specialists)
        for p in self.patients:
            actfact.generate(p, 30)
            
        shortstayfact=ShortStayFactory(self.context,self.etablissements)
        for p in self.patients:
            shortstayfact.generate(p, 4)
            
if __name__ == "__main__":
    sim = simulation(nomencl="/home/tguyet/Progs/medtrajectory_datagen/datarep/snds_nomenclature.db")
    sim.run()
    
    for p in sim.patients:
        for dd in p.drugdeliveries:
            print(dd)
        for dd in p.visits:
            print(dd)
        for dd in p.medicalacts:
            print(dd)
        for dd in p.hospitalStays:
            print(dd)
        print("========")
