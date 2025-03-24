from pymongo import MongoClient

# Connexion à MongoDB localement
client = MongoClient("mongodb://localhost:27017/")

# Accéder à la base de données "entertainment"
db = client["entertainment"]

# Tester la connexion en affichant les films (par exemple)
films_collection = db["films"]
films = films_collection.find().limit(5)
for film in films:
    print(film)

print("Connexion réussie à MongoDB!")
