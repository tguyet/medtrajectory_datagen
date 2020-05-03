#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  3 10:44:59 2020

@author: tguyet
"""

from database_model import Patient
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


class PatientFactory:
	def __init__(self,con):
		"""
		- con is a Factory context, which include a connexion to the nomenclature database
		"""
		self.context=con
	
	
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
			patients.append( p )
		
		return patients