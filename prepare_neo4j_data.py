from pymongo import MongoClient
from dotenv import load_dotenv
import os
import json

load_dotenv()

client = MongoClient(os.getenv("MONGODB_URI"))
db = client.entertainment
films = db.films

def extract_films_data():
    projection = {
        "_id": 1,
        "title": 1,
        "genre": 1,
        "Director": 1,
        "Actors": 1,
        "year": 1,
        "Runtime (Minutes)": 1,
        "Rating": 1,
        "Votes": 1,
        "Revenue (Millions)": 1,
        "Metascore": 1
    }
    
    films_data = []
    for film in films.find({}, projection):
        # On garde le genre tel quel comme une chaîne unique
        genre = film.get("genre", "").strip()
        
        cleaned_film = {
            "film_id": str(film["_id"]),
            "title": film.get("title", "Sans titre"),
            "genre": genre if genre else "Unknown",
            "director": film.get("Director", "Inconnu").strip(),
            "actors": [a.strip() for a in film.get("Actors", "").split(",") if a.strip()],
            "year": film.get("year", 0),
            "runtime": film.get("Runtime (Minutes)", 0),
            "rating": float(film["Rating"]) if film.get("Rating") else 0.0,
            "votes": int(film["Votes"]) if film.get("Votes") else 0,
            "revenue": float(film["Revenue (Millions)"]) if film.get("Revenue (Millions)") else 0.0,
            "metascore": film.get("Metascore", 0)
        }
        films_data.append(cleaned_film)
    return films_data

def save_to_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    films_data = extract_films_data()
    save_to_json(films_data, "films_data.json")
    print(f"Données extraites ({len(films_data)} films)")
    
    
"""
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import json

load_dotenv()

# Connexion à MongoDB
client = MongoClient(os.getenv("MONGODB_URI"))
db = client.entertainment
films = db.films

def extract_films_data():
    #Extrait et nettoie les données des films pour Neo4j
    projection = {
        "_id": 1,
        "title": 1,  # Note: minuscule dans le JSON
        "Genre": 1,
        "Director": 1,
        "Actors": 1,
        "year": 1,
        "Runtime (Minutes)": 1,
        "rating": 1,
        "Votes": 1,
        "Revenue (Millions)": 1,
        "Metascore": 1
    }
    
    films_data = []
    for film in films.find({}, projection):
        # Nettoyage des données
        film["film_id"] = str(film["_id"])
        film["title"] = film.get("title", "Sans titre")  # Champ title en minuscule
        film["director"] = film.get("Director", "Inconnu")
        film["actors"] = film.get("Actors", "").split(", ")
        films_data.append(film)
    film["revenue"] = float(film["Revenue (Millions)"]) if film.get("Revenue (Millions)") and str(film["Revenue (Millions)"]).strip() else 0.0
    film["rating"] = float(film["Rating"]) if film.get("Rating") and str(film["Rating"]).strip() else 0.0
    return films_data

def save_to_json(data, filename):
     #Sauvegarde les données en JSON
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    # 1. Extraction des données films
    films_data = extract_films_data()
    save_to_json(films_data, "films_data.json")
    print(f"Données des films extraites ({len(films_data)} films)")
"""