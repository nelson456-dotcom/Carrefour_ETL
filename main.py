# -*- coding: utf-8 -*-
from configparser import ConfigParser
from principale import Principale
import pandas as pd 
import glob
import os

######################################################
# read configuration from config file
######################################################

cfg = ConfigParser()
cfg.read('config.ini')


#data = list(dict(cfg.items('file')).values())

######################################################
# read csv files 
###################################################### 

path = os.getcwd() 
os.chdir(path + "\\input")

coding = "latin-1"


dataframe = {f[:-4]: pd.read_csv(f,sep=";",encoding='latin-1',skipinitialspace=True, low_memory=False) for f in glob.glob("*.csv")}
locals().update(dataframe)

data = ["Eptica ", "Sacha ", "Kiamo_", "Conso"]

try:
    for i in data:
         if i == 'Kiamo_':
             coding = "utf-8"
         else:
             coding = "latin-1"
         value = pd.concat(pd.read_csv(f, sep=";",encoding = coding, skipinitialspace=True, low_memory=False) for f in glob.glob('*{}*.csv'.format(i)))
         exec("{} = value".format('{}'.format(i)))
except Exception as e:
    print("impossible de trouver la fichier source !",  e)
    
prevision = pd.read_excel("Prévisions contacts CSC 2019 v2.xlsx", sep=";" ,encoding = "latin-1")

######################################################
#  upper case  
###################################################### 

Kiamo_.columns = ["Date d'entrée", 'Fin de traitement','Dernier Service','Dernier Agent','Dernière Qualification',"Durée totale d'attente",
                 'Durée totale de travail',"Durée de suivi","Numéro de téléphone client","SDA","Statut appel"]


Sacha['LibelleService'] = Sacha['LibelleService'].str.upper()
Sacha['Source']= Sacha['Source'].str.upper()
ref_sacha['LibelleService'] = ref_sacha['LibelleService'].str.upper()
ref_sacha_canal['Source']= ref_sacha_canal['Source'].str.upper()
Kiamo_['Dernier Service']= Kiamo_['Dernier Service'].str.upper()
ref_kiamo['Dernier Service']= ref_kiamo['Dernier Service'].str.upper()
prevision['Mission']= prevision['Mission'].str.upper()
prevision['Média']= prevision['Média'].str.upper()
ref_prevision['Mission']= ref_prevision['Mission'].str.upper()
ref_prevision_canal['Média']= ref_prevision_canal['Média'].str.upper()
Eptica['Origin']= Eptica['Origin'].str.upper()
ref_eptica['Origin']= ref_eptica['Origin'].str.upper()

######################################################
#  drop duplicate
###################################################### 
Eptica = Eptica.dropna(how = 'all') 
#prevision = prevision.drop_duplicates()
Eptica = Eptica.drop_duplicates()
Kiamo_ = Kiamo_.drop_duplicates()
Conso = Conso.drop_duplicates()
ref_prevision = ref_prevision.drop_duplicates()
ref_prevision_canal = ref_prevision_canal.drop_duplicates()
ref_sacha = ref_sacha.drop_duplicates()
ref_sacha_canal = ref_sacha_canal.drop_duplicates()
ref_eptica =ref_eptica.drop_duplicates()
ref_kiamo = ref_kiamo.drop_duplicates()
ref_sacha = ref_sacha.drop_duplicates()
ref_sacha_canal = ref_sacha_canal.drop_duplicates()
Sacha = Sacha.drop_duplicates(['Date de création', 'Date de clôture',
                                   'Date de la réponse de résolution initiale',
                                   'Statut','LibelleService','Source','E-mail','Numéro de téléphone 1',
                                   'Numéro de téléphone 2','Numéro de téléphone mobile','Profil'])

######################################################
# Traitement des tables volumes 
######################################################   

os.chdir(path)


consoweb_dim = Principale.consoweb(Conso)
kiamo_dim = Principale.kiamo(Kiamo_, ref_kiamo)
eptica_dim = Principale.eptica(Eptica, ref_eptica)
sacha_dim = Principale.sacha(Sacha, ref_sacha, ref_sacha_canal)
Principale.prevision(prevision, ref_prevision, ref_prevision_canal, consoweb_dim, sacha_dim, kiamo_dim, eptica_dim)