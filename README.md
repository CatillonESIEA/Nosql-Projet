# Projet NoSQL : MongoDB & Neo4j

# Objectifs du Projet :

Le but de ce TP / Projet est de se familiariser avec deux types de systèmes de gestion de bases de données NoSQL :

- MongoDB, une base de données orientée document

- Neo4j, une base de données orientée graphe

L'objectif est de développer une application en Python (avec Streamlit, par exemple) capable d’interagir avec ces bases de données NoSQL hébergées dans le cloud. L'application devra répondre aux questions et récupérer les données pertinentes.

# Description du Projet

L'application Python devra accomplir les tâches suivantes :

1. Connexion aux Bases de Données

Établir une connexion sécurisée avec les instances cloud de MongoDB et de Neo4j.

2. Interrogation de MongoDB

Effectuer des requêtes pour récupérer des documents spécifiques.

Implémenter des fonctions pour insérer, mettre à jour et supprimer des documents.

3. Interrogation de Neo4j

Utiliser le langage de requête Cypher pour interroger la base de données Neo4j.

Créer des nœuds, des relations et des propriétés.

Effectuer des recherches pour trouver :

Les chemins les plus courts

Des motifs récurrents

Des analyses de réseau

4. Analyse et Visualisation

Analyser les données récupérées à partir des requêtes.

Visualiser les résultats à l’aide de bibliothèques Python appropriées :

Matplotlib ou Seaborn pour MongoDB

Neovis.js pour Neo4j

# Résultats Attendus

À la fin de ce projet, vous devrez soumettre :

Un rapport de projet détaillant :

La démarche adoptée

Les requêtes utilisées

Les difficultés rencontrées et les solutions adoptées

Le code source de l’application, bien commenté pour expliquer votre logique

Technologies utilisées

Python (Streamlit, PyMongo, Neo4j)

MongoDB Atlas (Base de données NoSQL orientée document)

Neo4j AuraDB (Base de données NoSQL orientée graphe)

Matplotlib / Seaborn (Visualisation des données MongoDB)

Neovis.js (Visualisation des graphes Neo4j)

Installation

Cloner le dépôt GitHub

git clone https://github.com/votre-utilisateur/nom-du-projet.git
cd nom-du-projet

Créer un environnement virtuel et l'activer

python -m venv .venv
.venv\Scripts\activate     # Sur Windows

Installer les dépendances

pip install -r requirements.txt

Lancer l'application Streamlit

