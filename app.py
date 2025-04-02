import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pymongo import MongoClient
from neo4j_connection import Neo4jConnection

# Connexion à MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["entertainment"]
films_collection = db["films"]

# Titre principal
st.title("Application d'analyse de films")

# Fonction pour récupérer des films depuis MongoDB
def get_films(limit=10):
    return list(films_collection.find().limit(limit))

# Affichage des films dans une table
st.header("Liste des films")
films = get_films()
df_films = pd.DataFrame(films)
st.dataframe(df_films[['title', 'year', 'rating', 'genre']])

# Analyse des films par année
st.header("Distribution des films par année")
data = list(films_collection.aggregate([
    {"$group": {"_id": "$year", "count": {"$sum": 1}}},
    {"$sort": {"_id": 1}}
]))

if data:
    years = [d["_id"] for d in data]
    counts = [d["count"] for d in data]
    fig, ax = plt.subplots()
    sns.barplot(x=years, y=counts, ax=ax)
    ax.set_xlabel("Année")
    ax.set_ylabel("Nombre de films")
    ax.set_title("Nombre de films par année")
    st.pyplot(fig)

# Connexion à Neo4j et exploration des relations
def get_directors():
    conn = Neo4jConnection()
    driver = conn.connect()
    with driver.session() as session:
        result = session.run("MATCH (d:Director)-[:DIRECTED]->(f:Film) RETURN d.name AS director, count(f) AS films ORDER BY films DESC LIMIT 10")
        directors = result.data()
    conn.close()
    return directors

st.header("Top réalisateurs par nombre de films")
directors = get_directors()
df_directors = pd.DataFrame(directors)
st.dataframe(df_directors)
