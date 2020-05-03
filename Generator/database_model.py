#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  3 10:42:49 2020

@author: tguyet
"""

from datetime import date


class ALD:
	def __init__(self):
		self.motif="" #CIM10 medical motif
		self.start=date(1900,1,1)
		self.finish=date(2100,1,1)

class Patient:
	def __init__(self):
		self.NIR=0
		self.RNG_GEM=1
		self.Sex=0
		self.BD=date(1900,1,1) #birth day
		self.Dpt="01"
		self.City="0000"
		self.ALD=[] #list of ALD (see class ALD)
		
		
	def __str__(self):
		s="patient ("+self.NIR+") [sex: "+str(self.Sex)+", birth:"+self.BD.isoformat()+", loc:("+self.Dpt+","+self.City+")]"
		return s