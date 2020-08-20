#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Basic random data SNDS data simulator in our own data model.
This file contains a list of Factory classes that yield coherent instances of the classes in our data model.

@author: Thomas Guyet
@date: 08/2020
"""

from database_model import Patient, GP, Specialist, Provider, DrugDelivery, Etablissement, MedicalVisit, MedicalAct, ShortHospStay
import sqlalchemy as sa
import pandas as pd
import numpy as np
import numpy.random as rd
import string
from datetime import date, datetime,timedelta
import sqlite3 ##Here, I use sqlite to query the database (ie to access nomenclatures)



class FactoryContext:
    """
    class FactoryContext
    
    The class contains usefull tools to be used by different factories.
    -> a connexion to the database which enables to access the values that are used for some attributes of the database
    """    
    def __init__(self, nomenclatures="snds_nomenclature.db"):
        self.conn = sqlite3.connect( nomenclatures ) #connexion to the nomenclature database
        self.codes_geo=None

        
    def generate_date(self,begin = date(1900,1,1), end=date(2020,1,1)):
        """
        generate a random date between two dates
        
        TODO: define the notion of constraint for temporal constraintes
        """
        delta = timedelta( days=rd.randint(1, (end - begin).days+1) )
        return begin+delta
    
    def generate_location(self):
        """
        return a couple with a random city code and corresponding dpt code
        """
        if self.codes_geo is None:
            cur = self.conn.cursor()
            cur.execute("select GEO_DPT_COD, GEO_COM_COD from IR_GEO_V ;")
            self.codes_geo = cur.fetchall()
            cur.close()
        return self.codes_geo[ rd.randint(len(self.codes_geo)) ]


    def __genDpt2__(self):
        """
        generate a list of depts codes
        """
        dpts=["{:02}".format(i) for i in range(1,98)]
        dpts.append("2A")
        dpts.append("2B")
        dpts.append("99")
        return rd.choice(dpts)
    
class PharmacyFactory:
    def __init__(self, con):
        self.context=con
        
    def generate(self, n):
        """
        Generate n pharmacies including half GPs and half specialists
        """
        Pharmacies=[]
        for i in range(n):
            p=Provider()
            p.dpt = self.context.__genDpt2__()
            p.cat_nat=50 #pharmacie de ville
            p.id = str(p.dpt)+str(p.catpro)+"{:05}".format(rd.randint(99999))
            Pharmacies.append(p)
        return Pharmacies


class PhysicianFactory:
    def __init__(self, con):
        self.context=con


    def __generatePSNUM__(self,p):
        """
        generate a PSNUM that respect "https://documentation-snds.health-data-hub.fr/fiches/professionnel_sante.html"
        - 2 characters with dpt
        - 1 character with category
        - 5 random numbers
        """
        return str(p.dpt)+str(p.catpro)+"{:05}".format(rd.randint(99999))

    def generateGP(self, n):
        """
        Generate n General Practitioners
        """
        physicians=[]
        for i in range(n):
            p=GP()
            p.dpt = self.context.__genDpt2__()
            p.id = self.__generatePSNUM__(p)
            physicians.append(p)
        return physicians
            
    def generateSpecialists(self, n):
        """
        Generate n specialists (with random specialities)
        """
        
        #First get the list of specialities
        cur = self.context.conn.cursor()
        cur.execute("select PFS_SPE_COD from IR_SPE_V where PFS_SPE_COD>1 and PFS_SPE_COD<=85;")
        codes_spe = cur.fetchall()
        cur.close()
        codes_spe = [c[0] for c in codes_spe]
        
        physicians=[]
        for i in range(n):
            p=Specialist()
            p.dpt = self.context.__genDpt2__()
            p.speciality = rd.choice( codes_spe )#random choice of a speciality
            p.id = self.__generatePSNUM__(p)
            physicians.append(p)
        return physicians
            
            
    def generate(self, n):
        """
        Generate n physicians including half GPs and half specialists
        """
        physicians = self.generateGP(int(n/2))
        physicians += self.generateSpecialists(int(n/2))
        return physicians
        

class PatientFactory:
    def __init__(self,con,GPs=None):
        """
        - con is a Factory context, which include a connexion to the nomenclature database
        - GPs is a list of General practitioners or a list labels
        """
        self.context=con
        self.GPs=GPs
    
    
    def __generateNIR__(self,p):
        """
        generate the NIR of the patient
        """
        birth_location=self.context.generate_location()
        p.NIR=str(p.Sex)+"{:02}".format(p.BD.month)+"{:02}".format(p.BD.year%100)+birth_location[0]+birth_location[1]+"{:03}".format(rd.randint(999))
    
    def generate(self,n):
        """
        - n number of patients to generate
        """
        patients=[]
        for i in range(n):
            p=Patient()
            p.Sex=rd.choice([1,2,9],p=[0.495,0.495,0.01])
            p.BD=self.context.generate_date()
            p.Dpt,p.City=self.context.generate_location()
            self.__generateNIR__(p)
            
            if self.GPs:
                mtt=rd.choice(self.GPs)
                """
                if isinstance(mtt, GP):
                    p.MTT=mtt.id
                elif isinstance(mtt, str):
                    p.MTT=mtt
                """
                p.MTT=mtt
                
            patients.append( p )
        
        return patients
    

class DrugsDeliveryFactory:
    def __init__(self,con,Pharmacies):
        """
        - con is a Factory context, which include a connexion to the nomenclature database
        - Pharmacies is a list of Pharmacies that can deliver drugs
        """
        self.context=con
        self.Pharmacies=Pharmacies
        
        #First get the list of drugs (CIP13 codes)
        cur = self.context.conn.cursor()
        cur.execute("select PHA_CIP_C13 from IR_PHA_R;")
        self.cips = cur.fetchall()
        cur.close()
        self.cips = [c[0] for c in self.cips]
    
        
    def generate_one(self, p):
        """
        Generate a single drug delivery for a patient p
        The delivery is added to the patient it self
        
        - p: a patient to which generate a drug delivery
        """
        
        #generate a drug delivery with a random CIP
        cip13 = rd.choice(self.cips)
        dd=DrugDelivery(cip13, p, rd.choice(self.Pharmacies))
        
        dd.date_debut=self.context.generate_date(begin = p.BD, end=date(2020,1,1))
        dd.date_fin=dd.date_debut
        
        p.drugdeliveries.append(dd)
    
    def generate(self, p, maxdd=50):
        """
        Generate a drug delivery for a patient p
        The delivery is added to the patient itself
        
        - p: a patient to which generate a drug delivery
        - maxdd: maximal number of deliveries
        """
        for i in range(rd.randint(maxdd)):
            self.generate_one(p)


class EtablissementFactory:
    def __init__(self,con):
        self.context=con
    def generate(self):
        return Etablissement()


class VisitFactory:
    def __init__(self, con, physicians):
        self.context=con
        self.physicians=physicians
        
    def generate_one(self, p):
        """
        - p patient to which 
        """
        visit= MedicalVisit(p,p.MTT)
        
        visit.date_debut=self.context.generate_date(begin = p.BD, end=date(2020,1,1))
        visit.date_fin=visit.date_debut
        
        p.visits.append(visit)
        
    def generate(self, p, nbs=30):
        for i in range(nbs):
            self.generate_one(p)


class ActFactory:
    def __init__(self, con, physicians):
        self.context=con
        self.physicians = physicians
        
        cur = self.context.conn.cursor()
        cur.execute("SELECT CAM_PRS_IDE_COD FROM IR_CCAM_V54;")
        ccams = cur.fetchall()
        cur.close()
        self.ccams = [c[0] for c in ccams]
    
    def generate_one(self, p):
        """
        - p patient
        """
        ccam = rd.choice(self.ccams)
        spe = rd.choice(self.physicians)
        mact= MedicalAct(ccam, p,spe)
        mact.date_debut=self.context.generate_date(begin = p.BD, end=date(2020,1,1))
        mact.date_fin=mact.date_debut
        
        p.medicalacts.append(mact)

    def generate(self, p, nbs=30):
        """
        - p patient
        - nbs number of act to generate for a patient
        """
        for i in range(nbs):
            self.generate_one(p)

class ShortStayFactory:
    def __init__(self, con, hospitals):
        self.context=con
        
        #get the list of CIM codes
        cur = self.context.conn.cursor()
        cur.execute("SELECT CIM_COD FROM IR_CIM_V;")
        cims = cur.fetchall()
        cur.close()
        self.cims = [c[0] for c in cims]
        
        self.hospitals = hospitals
        
        """
        cur = self.context.conn.cursor()
        cur.execute("SELECT CAM_PRS_IDE_COD FROM IR_CCAM_V54;")
        ccams = cur.fetchall()
        cur.close()
        self.ccams = [c[0] for c in ccams]
        """
        
    
    def generate_one(self, p):
        """
        - p patient
        """
        DP = rd.choice(self.cims)
        e = rd.choice(self.hospitals)
        stay = ShortHospStay(p, e, DP )
        
        
        stay.start_date = self.context.generate_date(begin = p.BD, end=date(2020,1,1))
        stay.finish_date = self.context.generate_date(begin = stay.start_date, end=date(2020,1,1))
        
        #generate associated diagnosis (use aideaucodage.fr)
        for i in range(4):
            stay.cim_das.append( rd.choice(self.cims) )           # associate/related diagnosis
        """
        stay.ccam = []
        """
        p.hospitalStays.append(stay)
        
        
    def generate(self, p, nbs=3):
        """
        - p patient
        - nbs number of hospitalisation to generate for a patient
        """
        for i in range(nbs):
            self.generate_one(p)
