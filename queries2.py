from pymongo import MongoClient
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os

load_dotenv()
uri = os.getenv("MONGODB_URI")

client = MongoClient(
    uri,
    server_api=ServerApi('1'),
    socketTimeoutMS=60000,
    connectTimeoutMS=60000
)

try:
    client.admin.command('ping')
    print("Connexion réussie à MongoDB Atlas !")
except Exception as e:
    print(f"Erreur : {e}")

db = client["entertainment"]
films_collection = db["films"]

def most_films_year():
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    result = list(films_collection.aggregate(pipeline))
    return result[0] if result else None

def count_films_after_1999():
    return films_collection.count_documents({"year": {"$gt": 1999}})

def average_votes_2007():
    films_2007 = films_collection.find({"year": 2007})
    total_votes, count = 0, 0
    for film in films_2007:
        total_votes += film.get("Votes", 0)
        count += 1
    return total_votes / count if count > 0 else None

def plot_films_by_year():
    years = [film["year"] for film in films_collection.find() if "year" in film]
    if years:
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.histplot(years, kde=False, bins=30, color='blue', ax=ax)
        ax.set_title("Nombre de films par année")
        ax.set_xlabel("Année")
        ax.set_ylabel("Nombre de films")
        return fig
    return None

def list_genres():
    return films_collection.distinct("genre")

def film_avec_plus_de_revenus():
    top_revenue_film = films_collection.find({"Revenue (Millions)": {"$ne": ""}}).sort("Revenue (Millions)", -1).limit(1)
    return list(top_revenue_film)[0] if top_revenue_film else None

def directors_with_more_than_5_films():
    pipeline = [
        {"$group": {"_id": "$Director", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 5}}},
        {"$sort": {"count": -1}}
    ]
    return list(films_collection.aggregate(pipeline))

def genre_with_highest_avg_revenue():
    pipeline = [
        {"$group": {"_id": "$genre", "avg_revenue": {"$avg": "$Revenue (Millions)"}}},
        {"$sort": {"avg_revenue": -1}},
        {"$limit": 1}
    ]
    result = list(films_collection.aggregate(pipeline))
    return result[0] if result else None

def top_rated_films_by_decade():
    decades = ['1990-1999', '2000-2009', '2010-2019']
    result = {}
    for decade in decades:
        start_year, end_year = map(int, decade.split('-'))
        films = list(films_collection.find({"year": {"$gte": start_year, "$lte": end_year}, "rating": {"$ne": "unrated"}}).sort("rating", 1).limit(3))
        result[decade] = films
    return result

def longest_movie_by_genre():
    pipeline = [
        {"$unwind": "$genre"},
        {"$sort": {"Runtime (Minutes)": -1}},
        {"$group": {"_id": "$genre", "longest_movie": {"$first": "$title"}, "runtime": {"$first": "$Runtime (Minutes)"}}},
        {"$sort": {"_id": 1}}
    ]
    return list(films_collection.aggregate(pipeline))

def create_and_display_view():
    view_name = "high_rated_high_revenue_films"
    db[view_name].drop()
    db.command({
        "create": view_name,
        "viewOn": "films",
        "pipeline": [{"$match": {"Metascore": {"$gt": 80}, "Revenue (Millions)": {"$gt": 50}}}]
    })
    return list(db[view_name].find().limit(5))

def correlation_runtime_revenue():
    films = films_collection.find()
    runtimes, revenues = [], []
    for film in films:
        try:
            runtime = float(film.get("Runtime (Minutes)", 0))
            revenue = float(film.get("Revenue (Millions)", 0))
            if runtime > 0 and revenue > 0:
                runtimes.append(runtime)
                revenues.append(revenue)
        except (ValueError, TypeError):
            continue
    return np.corrcoef(runtimes, revenues)[0][1] if len(runtimes) > 1 and len(revenues) > 1 else None

def average_runtime_by_decade():
    decades = {"1990-1999": [], "2000-2009": [], "2010-2019": []}
    films = films_collection.find()
    for film in films:
        try:
            year = int(film.get("year", 0))
            runtime = float(film.get("Runtime (Minutes)", 0))
            if 1990 <= year <= 1999:
                decades["1990-1999"].append(runtime)
            elif 2000 <= year <= 2009:
                decades["2000-2009"].append(runtime)
            elif 2010 <= year <= 2019:
                decades["2010-2019"].append(runtime)
        except (ValueError, TypeError):
            continue
    return {decade: (sum(runtimes) / len(runtimes) if runtimes else None) for decade, runtimes in decades.items()}
