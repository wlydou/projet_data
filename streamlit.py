#!/usr/bin/env python
# coding: utf-8


# In[3]:

import pandas as pd
import sqlalchemy
import numpy as np
import pymysql
# import pymysql.cursors
import os
import streamlit as st

st.title("Flights")

# In[4]:


# con = sqlalchemy.create_engine("mysql+pymysql://root:root@localhost/trafic_aerien")
con = sqlalchemy.create_engine("mysql+pymysql://groupe4_python:peEen8T7EgYmFBi@db4free.net/trafic_aerien")

print('Connected')


# Q2. Combien y-a-t-il d’aéroports, de compagnies, de destinations, d’avions et de fuseaux horaires ?
st.subheader('Combien y-a-t-il d’aéroports, de compagnies, de destinations, d’avions et de fuseaux horaires ?')

# In[5]:


Q2_a = pd.read_sql_query('SELECT COUNT(DISTINCT faa) AS Aéroports FROM airports', con = con)
Q2_a



# In[6]:


Q2_b = pd.read_sql_query('SELECT COUNT( DISTINCT carrier) AS Compagnies FROM airlines', con = con)
Q2_b


# In[7]:


Q2_c = pd.read_sql_query('SELECT COUNT( DISTINCT dest) AS Destinations FROM flights', con = con)
Q2_c


# In[8]:


Q2_d = pd.read_sql_query('SELECT COUNT( DISTINCT tailnum) AS Avions FROM planes', con = con)
Q2_d


# In[9]:


Q2_e = pd.read_sql_query('SELECT COUNT( DISTINCT tz) AS Fuseaux_Horaires FROM airports ', con = con)
Q2_e


# Q3. Combien y-a-t-il de zones aux Etats-Unis où on ne passe pas à l’heure d’été (indice : colonne dst) ?
st.subheader('Combien y-a-t-il de zones aux Etats-Unis où on ne passe pas à l’heure d’été (indice : colonne dst) ?')
# In[10]:


Q3 = pd.read_sql_query("SELECT DISTINCT tzone AS Zones FROM airports WHERE dst = 'N'", con = con)
Q3


# Q4. Quel est l’aéroport de départ le plus emprunté ? Quelles sont les 10 destinations les plus (moins) prisées ? Quelle sont les 10 avions qui ont le plus (moins) décollé ?
st.subheader('Quel est l’aéroport de départ le plus emprunté ? Quelles sont les 10 destinations les plus (moins) prisées ? Quelle sont les 10 avions qui ont le plus (moins) décollé ?')

# In[11]:





Q4_a = pd.read_sql_query("SELECT origin, COUNT(origin) as Nbr FROM flights GROUP BY origin", con = con)
Q4_a


# In[12]:


Q4_b = pd.read_sql_query("SELECT dest, COUNT(dest) AS Plus_prisées from flights GROUP BY dest ORDER BY COUNT(dest) DESC LIMIT 0,10", con = con)
Q4_b

st.line_chart(Q4_b)
# In[13]:


Q4_c = pd.read_sql_query("SELECT dest, COUNT(dest) AS Moins_prisées from flights GROUP BY dest ORDER BY COUNT(dest) ASC LIMIT 0,10", con = con)
Q4_c
st.table(Q4_c)


# In[14]:


Q4_d = pd.read_sql_query("SELECT tailnum, COUNT(tailnum) AS Plus_décollés from flights GROUP BY tailnum ORDER BY COUNT(tailnum) DESC LIMIT 1,11", con = con)
Q4_d



# In[15]:


Q4_e = pd.read_sql_query("SELECT tailnum, COUNT(tailnum) AS Moins_décollés from flights GROUP BY tailnum ORDER BY COUNT(tailnum) ASC LIMIT 0, 10", con = con)
Q4_e


# Q5. Trouver combien chaque compagnie a desservi de destination ; combien chaque compagnie a desservie de destination par aéroport d’origine. Réaliser les graphiques adéquats qui synthétisent ces informations ?
st.subheader('Q.5 Trouver combien chaque compagnie a desservi de destination ; combien chaque compagnie a desservie de destination par aéroport d’origine. Réaliser les graphiques adéquats qui synthétisent ces informations ?')

# In[16]:


# Q5_a = pd.read_sql_query("SELECT airlines.carrier, airlines.name, count(dest) FROM flights INNER JOIN airlines ON flights.carrier = airlines.carrier GROUP BY carrier", con = con)
# Q5_a


# In[ ]:


# Q5_b = pd.read_sql_query("SELECT airlines.carrier, airlines.name, flights.origin, count(dest) FROM flights INNER JOIN airlines ON flights.carrier = airlines.carrier GROUP BY carrier, origin", con = con)
# Q5_b


# Q6. Trouver tous les vols ayant atterri à Houston (IAH ou HOU) (indice : 9313 vols).
# Combien de vols partent de NYC airports vers Seattle (indice : 3923 vols), combien de compagnies desservent cette destination (indice : 5 compagnies) et combien d’avions “uniques” (indice : 935 avions) ? 
st.subheader('Q.6 Trouver tous les vols ayant atterri à Houston (IAH ou HOU).Combien de vols partent de NYC airports vers Seattle (indice : 3923 vols), combien de compagnies desservent cette destination (indice : 5 compagnies) et combien d’avions “uniques” (indice : 935 avions) ? ')

# In[ ]:


q6_a = pd.read_sql_query('SELECT COUNT(dest) FROM `flights` WHERE dest = "IAH" OR dest = "HOU"', con = con)
q6_a


# In[ ]:


q6_b = pd.read_sql_query('SELECT COUNT(dest) FROM `flights` WHERE origin in (\'JFK\', \'EWR\', \'LGA\') AND dest = "SEA"', con = con)
q6_b


# In[ ]:


q6_c = pd.read_sql_query('SELECT COUNT(DISTINCT carrier) FROM `flights` WHERE origin in (\'JFK\', \'EWR\', \'LGA\') AND dest = "SEA"', con = con)
q6_c


# In[ ]:


q6_d = pd.read_sql_query('SELECT COUNT(DISTINCT tailnum) FROM `flights` WHERE origin in (\'JFK\', \'EWR\', \'LGA\') AND dest = "SEA"', con = con)
q6_d


# Q7.Trouver le nombre de vols unique par destination voir l’aperçu.
# Trier les vols suivant la destination, l’aéroport d’origine, la compagnie dans un ordre balphabétique croissant (en réalisant les jointures nécessaires pour obtenir les noms des explicites des aéroports) ?
st.subheader('Q7. Trouver le nombre de vols unique par destination voir l’aperçu. Trier les vols suivant la destination, l’aéroport d’origine, la compagnie dans un ordre balphabétique croissant (en réalisant les jointures nécessaires pour obtenir les noms des explicites des aéroports) ?')

# In[ ]:


q7 = pd.read_sql_query('SELECT airports.name, COUNT(DISTINCT flights.flight) as nb_vol_unique FROM flights INNER JOIN airports ON flights.dest = airports.faa GROUP BY flights.dest ORDER BY nb_vol_unique DESC', con = con)
q7


# Q8. Quelles sont les compagnies qui n'opèrent pas sur tous les aéroports d’origine ?
# Quelles sont les compagnies qui desservent l’ensemble de destinations ?
# 
# Faire un tableau où l’on récupère l’ensemble des origines et des destinations pour l’ensemble des compagnies.

# In[ ]:


q8_a = pd.read_sql_query('SELECT count(DISTINCT origin), flights.carrier, airlines.name, count(DISTINCT dest) FROM flights INNER JOIN airlines ON flights.carrier = airlines.carrier GROUP BY carrier ORDER BY count(DISTINCT dest) DESC', con = con)
q8_a


# Q9. Quelles sont les destinations qui sont exclusives à certaines compagnies?
st.subheader('Q.9 Quelles sont les destinations qui sont exclusives à certaines compagnies ?')
# In[ ]:


q9 = pd.read_sql_query('select dest, count(dest) AS \'nb_dest\', carrier from flights group by dest, carrier', con = con)


# In[ ]:


filteredq9 = q9.drop_duplicates(subset=['dest'], keep=False)
filteredq9["dest"].count()
filteredq9


# Q10. Filtrer le vol pour trouver ceux exploités par United, American ou Delta
st.subheader('Q.10 Filtrer le vol pour trouver ceux exploités par United, American ou Delta')

# In[ ]:


q10 = pd.read_sql_query('select * from flights where carrier in ("AA","UA","DL")', con = con)
q10

