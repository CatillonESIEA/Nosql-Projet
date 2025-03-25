from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()

# Récupérer l'URI depuis les variables d'environnement
uri = os.getenv("MONGODB_URI")

# Le reste du fichier reste inchangé...
# Client MongoDB avec paramètres de timeout
client = MongoClient(
    uri,
    server_api=ServerApi('1'),
    socketTimeoutMS=60000,  # Timeout des sockets
    connectTimeoutMS=60000  # Timeout de connexion
)

try:
    # Test Ping pour vérifier la connexion
    client.admin.command('ping')
    print("Connexion réussie à MongoDB Atlas !")
except Exception as e:
    print(f"Erreur : {e}")


# Accès à la base de données et à la collection "films"
db = client["entertainment"]
films_collection = db["films"]

# Affichage des 10 premiers films
try:
    films = films_collection.find().limit(10)  # Limité à 10 pour éviter trop d'informations
    for film in films:
        print(film)
except Exception as e:
    print(f"Erreur lors de l'affichage des films : {e}")


"""from pymongo import MongoClient

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
"""