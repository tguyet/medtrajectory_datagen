#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  3 10:44:59 2020

@author: tguyet
"""

from database_model import Patient, GP, Specialist, Provider
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
	def __init__(self):
		self.conn = sqlite3.connect("snds_nomenclature.db") #connexion to the nomenclature database
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
		Generate n physicians including half GPs and half specialists
		"""
		Pharmacies=[]
		for i in range(n):
			p=Provider()
			p.dpt = self.context.__genDpt2__()
			p.cat_nat=50 #pharmacie de ville
			p.id = p.dpt+"2{:05}".format(rd.randint(99999))
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
		return p.dpt+str(p.catpro)+"{:05}".format(rd.randint(99999))

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
				if isinstance(mtt, GP):
					p.MTT=mtt.id
				elif isinstance(mtt, str):
					p.MTT=mtt
				
			
			patients.append( p )
		
		return patients