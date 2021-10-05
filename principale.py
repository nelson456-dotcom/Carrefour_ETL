# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from pandas import DataFrame
from datetime import datetime
from datetime import date
import glob
import os
import re
import time


class Principale:

######################################################
# consoweb traitement 
#####################################################    
    
    def consoweb(consoweb):

        value = "01/"  
        consoweb["date"] = [value for i in consoweb["Créée le"]]    
        consoweb["Date_jour"] = consoweb["Créée le"].str.split(' ').str[0].tolist()
        consoweb["Date_mois"] =pd.Series(consoweb["date"]) + pd.Series(consoweb["Date_jour"]).str.split("/").str[1]+"/"+pd.Series(consoweb["Date_jour"]).str.split("/").str[2]
        consoweb["Flag_traite"] =  pd.Series(np.where((consoweb['Statut']== "Clos Complet"),1,0))   
        consoweb = consoweb.dropna(axis=0, subset=['Support Contact'])
    
        list = ["FAX & BULLETIN","FAX QUALITE","FAX SC","LETTRE","LETTRE AVEC AR","LETTRE SANS AR"]
    
        def my_func(value):
            if value not in list:
                return "Email"
    
    
        consoweb["canal"] = consoweb["Support Contact"].apply(my_func)
        consoweb = consoweb.dropna(subset=['canal'])       
        consoweb["activite"] = "Service Consommateur"
        consoweb["regroupement_activite"] = "Autres activités"
    
        date_format = "%d/%m/%Y %H:%M"
        list_date_1 = []
        consoweb["durée de travail"] = pd.Series(list_date_1)
    
        date_format = "%d/%m/%Y %H:%M"
        list_date_1 = []
    
        def date_difference(b,a):
            if('/' in str(a) and '/' in str(b)):
                a = datetime.strptime(a, date_format)
                b = datetime.strptime(b, date_format)
                delta = a-b
                days, seconds = delta.days, delta.seconds
                hours_used = days * 24 + seconds // 3600
                hours, remainder = divmod(delta.seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                return round(float(hours_used+minutes/60+seconds/3600),6)
    
        consoweb["durée de travail"] = consoweb.apply(lambda x: date_difference(x['Contact initial'],x['Date de clôture']),axis =1)
    
        def my_func_24(value1,value2):
            if value1 <= 24 and value2 == 1:
                return 1
            else:
                return 0
    
        consoweb['Flag_inf24'] = consoweb.apply(lambda x: my_func_24(x['durée de travail'], x['Flag_traite']), axis=1)
    
    
        def my_func_48(value1,value2):
            if value1 > 24 and value1 <= 48 and value2 == 1:
                return 1
            else:
                return 0
    
    
        consoweb["Flag_inf24_48"] = consoweb.apply(lambda x: my_func_48(x['durée de travail'], x['Flag_traite']), axis=1) 
        consoweb = consoweb.dropna(axis=0, subset=['canal'])
    
    
        id = (range(0,len(consoweb)))
    
        consoweb["id"] = id
        consoweb_dim = consoweb[['id','Date_jour','Date_mois','Flag_traite','Flag_inf24','Flag_inf24_48','durée de travail','activite','canal',"regroupement_activite"]]
        #consoweb_dim.to_csv("output\\consoweb.csv", index = False, sep=';', encoding='latin-1')
        return consoweb_dim

######################################################
#   Kiamo traitement 
#####################################################

    def kiamo(kiamo , ref_kiamo):

        #kiamo.columns = ["Date d'entrée", 'Fin de traitement','Dernier Service','Dernier Agent','Dernière Qualification',"Durée totale d'attente",'Durée totale de travail',"Durée de suivi","Numéro de téléphone client","SDA","Statut appel"]
        #kiamo = kiamo.drop_duplicates()
        #ref_kiamo = pd.read_csv('ref_kiamo.csv',sep=";",encoding='latin-1')

        ref_kiamo['Activite']= ref_kiamo['Activite'].str.strip()
        ref_kiamo['Dernier Service']= ref_kiamo['Dernier Service'].str.strip()
        ref_kiamo["Regroupement d'activite"]= ref_kiamo["Regroupement d'activite"].str.strip()

        value = "01/"
        id= pd.Series(range(0,len(kiamo)))
        kiamo["id"] = id
        kiamo["date"] = [value for i in kiamo["Date d'entrée"]]
        kiamo["Date_jour"] = kiamo["Date d'entrée"].str.split(' ').str[0].tolist()
        kiamo["Date_mois"] = (kiamo["date"]) +  (kiamo["Date_jour"]).str.split("/").str[1]+"/"+(kiamo["Date_jour"]).str.split("/").str[2]
        kiamo = kiamo.merge(ref_kiamo, on='Dernier Service', how='left')
        kiamo["canal"] = 'Téléphone'

        def mef_duree(champ):
            try: return int( float((re.sub("\D", "", champ))) > 0 )
            except: return 0

        kiamo["Flag_traite"] = kiamo["Durée totale de travail"].apply(mef_duree)


        def my_func_flag_attente(value1,value2):
            if int(re.sub("\D", "", value1)) <= 180 and int(re.sub("\D", "", value2)) >0:
                return 1
            else:
                return 0

        kiamo["Flag_attente"] = kiamo.apply(lambda x: my_func_flag_attente(x["Durée totale d'attente"], x["Durée totale de travail"]), axis=1)
        kiamo["durée de travail"] = kiamo["Durée totale de travail"]
        kiamo = kiamo.dropna(subset=['Activite'])
        kiamo_dim = kiamo[['id','Date_jour','Date_mois','Flag_traite','Flag_attente','durée de travail','Activite',"Regroupement d'activite",'canal']]
        kiamo_dim = kiamo_dim.rename(columns={"Activite": "activite", "Regroupement d'activite": "regroupement_activite" })

        # Exportation de fichier finale 
        #kiamo_dim.to_csv("output\\kiamo.csv", index = False, sep=';', encoding='latin-1')
        return kiamo_dim

######################################################
#   eptica traitement 
#####################################################

    def eptica(eptica, ref_eptica):

        def change_type(column, data_type):
            column = pd.to_numeric(column, errors='coerce')
            column = column.fillna(0.0).astype(int)
            column = column.replace("NaN", "", regex=True)
            column = column.replace("NA", "", regex=True)
            column = column.apply(pd.to_numeric)
            column = column.astype(data_type)
            return column
            
        value = "01/"
        eptica["id"] = (range(0,len(eptica["CustomerId"])))
        eptica["date"] = [value for i in eptica["CreationDate"]]    
        eptica["Date_jour"] = eptica["CreationDate"].str.split(' ').str[0].tolist()
        eptica["Date_mois"] =pd.Series(eptica["date"]) + pd.Series(eptica["Date_jour"]).str.split("/").str[1]+"/"+pd.Series(eptica["Date_jour"]).str.split("/").str[2]
        
        #jointure ref_eptica
        eptica = eptica.merge(ref_eptica, on = "Origin", how = 'left')
        eptica["canal"] = "Email"
        eptica["ProcTime"] = change_type(eptica["ProcTime"], float)
        eptica["durée de travail"] =  round(eptica["ProcTime"].divide(3600),3)
        eptica["Flag_traite"] = (eptica["ReqState"] == 1).astype(int)

        #calcule des flag_inf24 et 48
        eptica["Flag_inf24"] = (((eptica['Flag_traite']== 1) & (eptica["durée de travail"] <= 24.0))).astype(int)
        eptica["Flag_inf24_48"] = ((eptica["durée de travail"] > 24.0) & (eptica['Flag_traite']== 1) & (eptica["durée de travail"] <= 48.0)).astype(int)
        eptica = eptica.dropna(subset=['activite'])
        eptica_dim = eptica[['id','Date_jour','Date_mois','Flag_inf24','Flag_traite','Flag_inf24_48','durée de travail','activite','canal','Regroupement Activite']]         
        eptica_dim = eptica_dim.rename(columns={"Regroupement Activite":"regroupement_activite"}) 
        
        #exportation du fichier csv
        #eptica_dim.to_csv("output\\eptica.csv", index = False, sep=';', encoding='latin-1')
        return eptica_dim

######################################################
#   sacha traitement 
#####################################################

    def sacha(sacha, ref_sacha, ref_sacha_canal):
        # initialisation date
        value = "01/"
        sacha["date"] = [value for i in sacha["Date de création"]]
        sacha["Date_jour"] = sacha["Date de création"].str.split(' ').str[0].tolist()
        sacha["Date_mois"] = sacha["date"] + sacha["Date_jour"].str.split("/").str[1]+"/"+sacha["Date_jour"].str.split("/").str[2]
        sacha = sacha.merge(ref_sacha, on='LibelleService', how='left')
        sacha = sacha.merge(ref_sacha_canal, on='Source', how='left')

        date_format = "%d/%m/%Y %H:%M:%S"


        def date_difference(b,a):
            if('/' in str(a) and '/' in str(b)):
                a = datetime.strptime(a, date_format)
                b = datetime.strptime(b, date_format)
                delta = a-b
                days, seconds = delta.days, delta.seconds
                hours_used = days * 24 + seconds // 3600
                hours, remainder = divmod(delta.seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                return round(float(hours_used+minutes/60+seconds/3600),6)

        sacha["durée de travail"] = sacha.apply(lambda x: date_difference(x["Date de création"],x["Date de clôture"]),axis =1)
        sacha["Flag_traite"] = ((sacha['Statut']== "Résolu") | (sacha['Statut']== "Résolu traité")).astype(int)
        sacha["Flag_inf24"] = ((sacha["Flag_traite"]== 1) & (sacha["durée de travail"] <= 24.00)).astype(int) 
        sacha["Flag_inf24_48"] = ((sacha["Flag_traite"]== 1) & (sacha["durée de travail"] <=48.00) & (sacha["durée de travail"] >24.00)).astype(int)

        sacha = sacha.dropna(subset=['activite'])
        sacha = sacha.dropna(subset=['canal'])

        sacha["id"] = (range(0,len(sacha["activite"])))
        sacha_dim = sacha[['id','Date_jour','Date_mois','Flag_traite','Flag_inf24','Flag_inf24_48','durée de travail','activite',"Regroupement Activite", 'canal']]
        sacha_dim = sacha_dim.rename(columns={"Regroupement Activite": "regroupement_activite"})  
        
        #sacha_dim.to_csv("output\\sacha.csv", index = False, sep=';', encoding='latin-1')
        return sacha_dim

######################################################
#   prevision traitement 
#####################################################

    def prevision(prevision, ref_prevision, ref_prevision_canal, consoweb_dim, sacha_dim, kiamo_dim, eptica_dim):
        
        def change_type(column, data_type):
            column = pd.to_numeric(column, errors='coerce')
            column = column.fillna(0.0).astype(int)
            column = column.replace("NaN", "", regex=True)
            column = column.replace("NA", "", regex=True)
            column = column.apply(pd.to_numeric)
            column = column.astype(data_type)
            return column

        prevision = prevision.drop_duplicates()  
        #prevision = prevision[~prevision["Média"].str.contains("traité")] 
        #prevision = prevision[~prevision["Média"].str.contains("Traité")] 
        prevision_transposed = prevision[['Janvier','Février','Mars','Avril','Mai','Juin',
                    'Juillet','Août','Septembre','Octobre','Novembre','Décembre']].transpose()
        columns = ['Date_mois','nb_prevu','Mission','Média']
        prevision_dim = pd.DataFrame(columns=columns)
        prevision_transposed=prevision_transposed.reset_index()
        prevision_transposed=prevision_transposed.drop(['index'],axis=1)

        prevision_transposed = prevision_transposed.rename(columns={x:y for x,y in zip(prevision_transposed.columns,range(0,len(prevision_transposed.columns)))}) 
 
        #prevision_transposed.to_csv("output\\test3.csv",sep=";",encoding='latin-1', index = False)

        dir_list= os.listdir(os.getcwd()+"\\input")

        for item in dir_list:
            if ("Prévision" in item):
                file = item
                
        value = file.find("20")
        year = str(file)[value:value+4]

        i=1
        list_mois = []
        while i <= 12:
            if i <10:
                list_mois.append("01"+"/"+"0"+str(i)+"/"+str(year))
            else: 
                list_mois.append("01"+"/"+str(i)+"/"+str(year))
            i+=1

        # mois,data,mission et média
        prevision_dim["Date_mois"] = list_mois * len(prevision)

        data_prevision = []
        i=0
        while i <len(prevision):
            data_prevision.extend((prevision_transposed[i]))
            i+=1
        prevision_dim["nb_prevu"] = data_prevision

        media = []
        i=0
        while i<len(prevision):
            j=0
            while(j)<12:
                media.append(prevision["Média"].iloc[i])
                j+=1
            i+=1
            
        prevision_dim["Média"] = media

        mission = []
        i=0
        while i<len(prevision):
            j=0
            while(j)<12:
                mission.append(prevision["Mission"].iloc[i])
                j+=1
            i+=1

        prevision_dim["Mission"] = mission
                
        prevision_dim  = prevision_dim.merge(ref_prevision, on = 'Mission',how='left') 
        prevision_dim = prevision_dim[pd.notnull(prevision_dim['activite'])]       
        prevision_dim = prevision_dim.merge(ref_prevision_canal, on = 'Média', how='left')        
        prevision_dim = prevision_dim[pd.notnull(prevision_dim['canal'])]
    
        # groupby nb_prevu selon date, canal, activite et regroupement activite
        prevision_dim = pd.DataFrame({'nb_prevu' : prevision_dim.groupby(['Date_mois','canal','activite','Regroupement Activite'])["nb_prevu"].sum()}).reset_index()
        prevision_dim = prevision_dim.rename(columns={'Regroupement Activite': 'regroupement_activite'})
        
        #prevision_dim = prevision_dim[pd.notnull(prevision_dim['canal'])]
        
        #prevision_dim = prevision_dim.drop("Mission",axis = 1)
        #prevision_dim = prevision_dim.drop("Média",axis = 1) 
        #prevision_dim = prevision_dim.drop_duplicates()
        
        table_intermediaire = pd.concat([kiamo_dim,consoweb_dim,sacha_dim,eptica_dim],sort=False)
        #table_intermediaire.to_csv("output\\table_intermediaire.csv",sep=";",encoding='latin-1',  index = False)
        groupby = pd.DataFrame({'nb_recu' : table_intermediaire.groupby(['Date_mois','canal','activite','regroupement_activite'])['id'].count()}).reset_index()
        
        print("the lenght of groupby and table intermediare is : : : ", len(groupby),len(prevision_dim))

   
        volume_contact = groupby[['Date_mois','canal','activite','regroupement_activite','nb_recu']].merge(prevision_dim[['Date_mois',
                        'canal','activite','regroupement_activite','nb_prevu']], on=['Date_mois','canal','activite','regroupement_activite'], how='outer')
            
        volume_contact.to_csv("output\\volume_contact.csv",sep=";",encoding='latin-1', index = False)
        
        columns = ["date_mois","Regroupement Activite","Canal","nb_recu",'nb_prevu','Activite']
        volume_contact_dim = pd.DataFrame(columns=columns)
        volume_contact_dim["date_mois"] = volume_contact["Date_mois"]
        volume_contact_dim["Regroupement Activite"] = volume_contact["regroupement_activite"]
        volume_contact_dim["Canal"] = volume_contact["canal"]
        volume_contact_dim["Activite"] = volume_contact["activite"]
        volume_contact_dim["nb_recu"] = volume_contact["nb_recu"]
        volume_contact_dim["nb_prevu"] = volume_contact["nb_prevu"]
        #volume_contact_dim = volume_contact_dim.drop_duplicates(["nb_recu"])
        
        #Table QoS
        table_intermediaire_qs = pd.concat([consoweb_dim,sacha_dim,eptica_dim], ignore_index = True,sort=False)
        table_intermediaire_qs["Flag_traite"] = change_type(table_intermediaire_qs["Flag_traite"],int)
        table_intermediaire_qs["Flag_inf24"] = change_type(table_intermediaire_qs["Flag_inf24"],int)
        table_intermediaire_qs["Flag_inf24_48"] = change_type(table_intermediaire_qs["Flag_inf24_48"],int)

        qos_email = table_intermediaire_qs[table_intermediaire_qs["canal"] == 'Email'].groupby(['Date_jour','canal','activite','regroupement_activite']).agg({"id":np.size,
                                          'Flag_traite': np.sum, 'Flag_inf24': np.sum ,'Flag_inf24_48':np.sum}).reset_index()

        qos_telephonie = kiamo_dim.groupby(['Date_jour','canal','activite','regroupement_activite']).agg({"id":np.size,'Flag_attente':np.sum,'Flag_traite': np.sum}).reset_index()


                
        columns = ["date_jour","Canal","Activites","Regroupement Activite",'nb_recu','nb_traite','nb_SL']
        qos_telephonie_dim = pd.DataFrame(columns=columns)
        qos_telephonie_dim["date_jour"] = qos_telephonie["Date_jour"]
        qos_telephonie_dim["Canal"] = qos_telephonie["canal"]
        qos_telephonie_dim["Activites"] = qos_telephonie["activite"]
        qos_telephonie_dim["Regroupement Activite"] = qos_telephonie["regroupement_activite"]
        qos_telephonie_dim["nb_recu"] =  qos_telephonie["id"]
        qos_telephonie_dim["nb_traite"] =  qos_telephonie["Flag_traite"]
        qos_telephonie_dim["nb_SL"] =  qos_telephonie["Flag_attente"]
        columns = ["Date_jour","canal","Activites","Regroupement Activite",'nb_recu','nb_traite','nb_inf24','nb_inf48']
        
        

        qos_email_dim = pd.DataFrame(columns=columns)

        qos_email_dim["Date Jour"] = qos_email["Date_jour"]
        qos_email_dim["canal"] = qos_email["canal"]
        qos_email_dim["Activites"] = qos_email["activite"]
        qos_email_dim["Regroupement Activite"] = qos_email["regroupement_activite"] 
        qos_email_dim["nb_recu"] =  qos_email["id"]
        qos_email_dim["nb_traite"] =  qos_email["Flag_traite"]
        qos_email_dim["nb_inf24"] =  qos_email["Flag_inf24"]
        qos_email_dim["nb_24_48"] =  qos_email["Flag_inf24_48"]
        #Exportation des tables volumes
        volume_contact_dim.to_csv("output\\volume_contact.csv",sep=";",encoding='latin-1', index = False)
        qos_email_dim.to_csv("output\\qos_email.csv", sep=";",encoding='latin-1',  index = False)
        qos_telephonie_dim.to_csv("output\\qos_telephonie.csv",sep=";",encoding='latin-1',  index = False)
"""
    def mail(adress, subject, body):

        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.To = 'To address'
        mail.Subject = 'Message subject'
        mail.Body = 'Message body'
        mail.HTMLBody = '<h2>HTML Message body</h2>' #this field is optional
        # To attach a file to the email (optional):
        attachment  = "Path to the attachment"
        mail.Attachments.Add(attachment)
        mail.Send()

"""
