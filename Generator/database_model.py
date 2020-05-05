#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May  3 10:42:49 2020

@author: tguyet
"""

from datetime import date


#########################################
# https://documentation-snds.health-data-hub.fr/fiches/professionnel_sante.html

class PS: #praticien de soins
	def __init__(self):
		self.id="00000000000" #PFS_PFS_NUM: c'est le numéro du cabinet du praticien à 8 chiffres (2 chiffre dpt+3eme comme categorie professionnelle) ! (numéro PS officiel à 11 chiffres ... pas encore en place)
		self.dpt="" #code dpt a 2 chiffres !! (different de IR_DPT_V)
		self.catpro=9 # voir IR_CAT_V
		

class Physician(PS):
	"""
	categorie professionnelle 1: médecins
	"""
	def __init__(self):
		PS.__init__(self)
		self.speciality=0 #PSP_SPE_COD (prescripteur), voir IR_SPE_V, 0="non renseigné" (plutôt que PSE_SPE_COD (executant))
		self.catpro=1
		
	def __str__(self):
		return "Physician ("+str(self.id)+") [spe: "+str(self.speciality)+"]"
		
class GP(Physician):
	def __init__(self):
		Physician.__init__(self)
		self.speciality=1 #PSP_SPE_COD (prescripteur), voir IR_SPE_V, 1="Médecine Generaliste" (plutôt que PSE_SPE_COD (executant))
	

class Specialist(Physician):
	def __init__(self):
		Physician.__init__(self)
		self.speciality=0  #valeur entre 2 et 85 dans la liste IR_SPE_V (virer le 36: dentiste ???)

class Provider(PS):	
	"""
	categorie professionnelle 2: fournisseurs / pharmacies / transporteurs 
	"""
	def __init__(self):
		PS.__init__(self)
		self.speciality=None #PSE_SPE_COD (executant)
		self.cat_nat=50 #PSE_ACT_NAT (nature), voir codes dans IR_ACT_V[PFS_ACT_NAT] -> pharmacie 50
		self.catpro=2
	def __str__(self):
		return "Care Provider ("+str(self.id)+") [cat: "+str(self.cat_nat)+"]"
		
######################################

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
		self.MTT=None #médecin traitant
		
		
	def __str__(self):
		s="patient ("+self.NIR+") [sex: "+str(self.Sex)+", birth:"+self.BD.isoformat()+", loc:("+self.Dpt+","+self.City+")]"
		return s