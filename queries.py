from pymongo import MongoClient
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Connexion à MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Assurez-vous que l'URL est correcte pour votre connexion
db = client['entertainment']  # Utilisation de la base de données 'entertainment'
films_collection = db['films']  # Accès à la collection 'films'


# 1. Afficher l'année où le plus grand nombre de films ont été sortis
def most_films_year():
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    result = films_collection.aggregate(pipeline)
    for r in result:
        print(f"1. L'année où le plus grand nombre de films ont été sortis est {r['_id']} avec {r['count']} films.")


# 2. Quel est le nombre de films sortis après l'année 1999 ?
def count_films_after_1999():
    count = films_collection.count_documents({"year": {"$gt": 1999}})
    print(f"2. Nombre de films sortis après 1999 : {count}")


# 3. Quelle est la moyenne des votes des films sortis en 2007 ?
def average_votes_2007():
    films_2007 = films_collection.find({"year": 2007})
    total_votes = 0
    count = 0
    for film in films_2007:
        total_votes += film["Votes"]
        count += 1
    if count > 0:
        average_votes = total_votes / count
        print(f"3. La moyenne des votes des films sortis en 2007 est {average_votes:.2f}")
    else:
        print("3. Aucun film trouvé pour l'année 2007.")


# 4. Afficher un histogramme pour visualiser le nombre de films par année
def plot_films_by_year():
    # On s'assure que chaque film a bien un champ 'year'
    years = [film["year"] for film in films_collection.find() if "year" in film]
    if years:  # Si la liste 'years' n'est pas vide
        print("4. voir histogramme")
        plt.figure(figsize=(10, 6))
        sns.histplot(years, kde=False, bins=30, color='blue')
        plt.title("Nombre de films par année")
        plt.xlabel("Année")
        plt.ylabel("Nombre de films")
        plt.show()
    else:
        print("4. Aucun film avec l'année disponible.")



# 5. Quels sont les genres de films disponibles dans la base ?
def list_genres():
    genres = films_collection.distinct("genre")
    print(f"5. Genres de films disponibles : {', '.join(genres)}")


# 6. Le film ayant généré le plus de revenu
def film_avec_plus_de_revenus():
    # Recherche du film avec le plus grand revenu (en millions) qui n'a pas de valeur vide
    top_revenue_film = films_collection.find(
        {"Revenue (Millions)": {"$ne": ""}}  # Filtrer les films sans valeur vide pour le revenu
    ).sort("Revenue (Millions)", -1).limit(1)  # Trier par revenu décroissant et prendre le premier
    
    for film in top_revenue_film:
        print(f"6. Le film ayant généré le plus de revenu est {film['title']} avec {film['Revenue (Millions)']} millions de dollars.")



# 7. Quels sont les réalisateurs ayant réalisé plus de 5 films dans la base de données ?
def directors_with_more_than_5_films():
    pipeline = [
        {"$group": {"_id": "$Director", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 5}}},
        {"$sort": {"count": -1}}
    ]
    result = films_collection.aggregate(pipeline)
    print("7. Réalisateurs ayant réalisé plus de 5 films :")
    for r in result:
        print(f"7. Réalisateur : {r['_id']} - Nombre de films : {r['count']}")


# 8. Quel est le genre de film qui rapporte en moyenne le plus de revenus ?
def genre_with_highest_avg_revenue():
    pipeline = [
        {"$group": {"_id": "$genre", "avg_revenue": {"$avg": "$Revenue (Millions)"}}},
        {"$sort": {"avg_revenue": -1}},
        {"$limit": 1}
    ]
    result = films_collection.aggregate(pipeline)
    for r in result:
        print(f"8. Le genre qui rapporte en moyenne le plus de revenus est {r['_id']} avec {r['avg_revenue']:.2f} millions de dollars.")


# 9. Quels sont les 3 films les mieux notés pour chaque décennie ?
# Requête 9 : Films les mieux notés de chaque décennie
# 9. Films les mieux notés de la décennie 1990-1999, 2000-2009 et 2010-2019
def top_rated_films_by_decade():
    decades = ['1990-1999', '2000-2009', '2010-2019']
    print("9.")
    for decade in decades:
        start_year = int(decade.split('-')[0])
        end_year = int(decade.split('-')[1])
        
        # Requête pour les films de la décennie donnée et filtrage des films notés 'G'
        films = films_collection.find({
            "year": {"$gte": start_year, "$lte": end_year},
            "rating": {"$ne": "unrated"},  # Ignore les films non notés
            "rating": {"$ne": None}  # Ignore les films avec un champ 'rating' vide
        }).sort("rating", 1)  # Trie par rating, de A à Z (G avant unrated)
        
        # Afficher les 3 films les mieux notés de cette décennie
        print(f"\nFilms les mieux notés de la décennie {decade}:")
        top_films = list(films)[:3]  # Limite à 3 films
        if top_films:
            for film in top_films:
                print(f"{film['title']} - Note : {film['rating']}")
        else:
            print("Aucun film trouvé avec des évaluations valides.")

        


# 10. Quel est le film le plus long par genre ?
def longest_movie_by_genre():
    pipeline = [
        {"$unwind": "$genre"},  # Sépare les genres pour traiter chaque genre indépendamment
        {"$sort": {"Runtime (Minutes)": -1}},  # Trie par durée décroissante
        {"$group": {
            "_id": "$genre",  # Regroupe par genre
            "longest_movie": {"$first": "$title"},  # Prend le premier film (le plus long)
            "runtime": {"$first": "$Runtime (Minutes)"}  # Prend sa durée
        }},
        {"$sort": {"_id": 1}}  # Trie les genres par ordre alphabétique
    ]

    result = films_collection.aggregate(pipeline)

    print("10. Film le plus long par genre :")
    for r in result:
        print(f"Genre : {r['_id']} - Film : {r['longest_movie']} - Durée : {r['runtime']} min")


# 11. Fonction pour créer et afficher la vue MongoDB
def create_and_display_view():
    view_name = "high_rated_high_revenue_films"
    
    try:
        # Supprimer la vue si elle existe déjà (évite les erreurs)
        db[view_name].drop()

        # Création de la vue dans MongoDB
        db.command({
            "create": view_name,
            "viewOn": "films",
            "pipeline": [
                {"$match": {
                    "Metascore": {"$gt": 80}, 
                    "Revenue (Millions)": {"$gt": 50}
                }}
            ]
        })
        
        print(f"11. Vue '{view_name}' créée avec succès !")

        # Vérifier si la vue contient des films et en afficher quelques-uns
        films = db[view_name].find().limit(5)
        print("Films de la vue :")
        found = False
        for film in films:
            print(f"- {film['title']} | Metascore: {film['Metascore']} | Revenue: {film['Revenue (Millions)']} M$")
            found = True
        if not found:
            print("Aucun film ne correspond aux critères.")
    
    except Exception as e:
        print(f"Erreur lors de la création de la vue : {e}")


# 12. Corrélation entre le temps de runtime et les revenus
def correlation_runtime_revenue():
    # Récupérer tous les films
    films = films_collection.find()
    
    runtimes = []
    revenues = []
    
    for film in films:
        # Vérifier si les valeurs sont numériques (et non vides)
        try:
            runtime = float(film.get("Runtime (Minutes)", 0))  # Convertir en float, défaut à 0 si pas trouvé
            revenue = float(film.get("Revenue (Millions)", 0))  # Convertir en float, défaut à 0 si pas trouvé
            
            # Si runtime ou revenue sont 0 ou non valides, on les ignore
            if runtime > 0 and revenue > 0:
                runtimes.append(runtime)
                revenues.append(revenue)
        except (ValueError, TypeError):
            continue  # Ignore les films avec des valeurs non numériques

    # Calculer la corrélation entre runtime et revenue
    if len(runtimes) > 1 and len(revenues) > 1:
        correlation = np.corrcoef(runtimes, revenues)[0][1]
        print(f"12. Corrélation entre le temps de runtime et les revenus : {correlation}")
    else:
        print("12. Pas assez de données valides pour calculer la corrélation.")



# 13. Durée moyenne des films par décennie
def average_runtime_by_decade():
    # Regrouper les films par décennie
    decades = {
        "1990-1999": [],
        "2000-2009": [],
        "2010-2019": []
    }
    
    # Récupérer tous les films
    films = films_collection.find()
    print("13.")
    for film in films:
        try:
            year = int(film.get("year", 0))  # Convertir l'année en entier
            runtime = float(film.get("Runtime (Minutes)", 0))  # Convertir la durée en float
            
            # Ajouter le film dans la décennie appropriée
            if 1990 <= year <= 1999:
                decades["1990-1999"].append(runtime)
            elif 2000 <= year <= 2009:
                decades["2000-2009"].append(runtime)
            elif 2010 <= year <= 2019:
                decades["2010-2019"].append(runtime)
        except (ValueError, TypeError):
            continue  # Ignore les films avec des valeurs invalides
    
    # Calculer et afficher la durée moyenne pour chaque décennie
    for decade, runtimes in decades.items():
        if runtimes:
            avg_runtime = sum(runtimes) / len(runtimes)
            print(f"Durée moyenne des films de la décennie {decade} : {avg_runtime:.2f} minutes")
        else:
            print(f"Aucun film trouvé dans la décennie {decade}.")



# Appels des fonctions pour les tests
most_films_year()
print("")
count_films_after_1999()
print("")
average_votes_2007()
print("")
plot_films_by_year()
print("")
list_genres()
print("")
film_avec_plus_de_revenus()
print("")
directors_with_more_than_5_films()
print("")
genre_with_highest_avg_revenue()
print("")
top_rated_films_by_decade()
print("")
longest_movie_by_genre()
print("")
create_and_display_view()
print("")
correlation_runtime_revenue()
print("")
average_runtime_by_decade()
