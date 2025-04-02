import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from queries2 import (
    most_films_year,
    count_films_after_1999,
    average_votes_2007,
    plot_films_by_year,
    list_genres,
    film_avec_plus_de_revenus,
    directors_with_more_than_5_films,
    genre_with_highest_avg_revenue,
    top_rated_films_by_decade,
    longest_movie_by_genre,
    create_and_display_view,
    correlation_runtime_revenue,
    average_runtime_by_decade
)
from neo4j_queries2 import Neo4jQueries  # Importation des requêtes Neo4j

# Titre principal
st.title("Application d'analyse de films")

# 1. Année avec le plus grand nombre de films
st.header("Année avec le plus grand nombre de films")
st.write(most_films_year())

# 2. Nombre de films après 1999
st.header("Nombre de films sortis après 1999")
st.write(count_films_after_1999())

# 3. Moyenne des votes des films de 2007
st.header("Moyenne des votes des films sortis en 2007")
st.write(average_votes_2007())

# 4. Histogramme du nombre de films par année
st.header("Distribution des films par année")
fig = plot_films_by_year()
if fig:
    st.pyplot(fig)

# 5. Liste des genres de films
st.header("Genres de films disponibles")
st.write(list_genres())

# 6. Film ayant généré le plus de revenus
st.header("Film ayant généré le plus de revenus")
st.write(film_avec_plus_de_revenus())

# 7. Réalisateurs ayant fait plus de 5 films
st.header("Réalisateurs ayant réalisé plus de 5 films")
st.write(directors_with_more_than_5_films())

# 8. Genre avec la moyenne de revenus la plus élevée
st.header("Genre avec la moyenne de revenus la plus élevée")
st.write(genre_with_highest_avg_revenue())

# 9. Films les mieux notés de chaque décennie
st.header("Films les mieux notés par décennie")
st.write(top_rated_films_by_decade())

# 10. Film le plus long par genre
st.header("Film le plus long par genre")
st.write(longest_movie_by_genre())

# 11. Création et affichage de la vue MongoDB
st.header("Vue des films bien notés et rentables")
st.write(create_and_display_view())

# 12. Corrélation entre la durée et les revenus
st.header("Corrélation entre la durée des films et les revenus")
st.write(correlation_runtime_revenue())

# 13. Durée moyenne des films par décennie
st.header("Durée moyenne des films par décennie")
st.write(average_runtime_by_decade())

# Requêtes Neo4j
neo_queries = Neo4jQueries()

# 14. Acteur le plus prolifique
st.header("Acteur le plus prolifique")
st.write(neo_queries.get_most_prolific_actor())

# 15. Acteurs avec Anne Hathaway
st.header("Acteurs ayant joué avec Anne Hathaway")
st.write(neo_queries.get_actors_with_anne_hathaway())

# 16. Acteur avec le plus grand revenu total
st.header("Acteur ayant le revenu total le plus élevé")
st.write(neo_queries.get_actor_with_highest_total_revenue())

# 17. Moyenne des votes de films
st.header("Moyenne des votes de tous les films")
st.write(neo_queries.get_average_votes())

# 18. Genre le plus commun
st.header("Genre de film le plus commun")
st.write(neo_queries.get_most_common_genre())

# 19. Films des co-acteurs (par défaut Matt Damon)
st.header("Films des co-acteurs de Matt Damon")
st.write(neo_queries.get_coactors_films("Matt Damon"))

# 20. Réalisateur avec le plus d'acteurs
st.header("Réalisateur ayant travaillé avec le plus d'acteurs")
st.write(neo_queries.get_director_most_actors())

# 21. Films les plus connectés
st.header("Films les plus connectés par les acteurs")
st.write(neo_queries.get_most_connected_films())

# 22. Acteurs ayant travaillé avec le plus de réalisateurs
st.header("Acteurs ayant travaillé avec le plus de réalisateurs")
st.write(neo_queries.get_actors_most_directors())

# 23. Recommandation de film pour Tom Hanks
st.header("Recommandation de film pour Tom Hanks")
st.write(neo_queries.recommend_movie_for_actor("Tom Hanks"))

# 24. Création des relations d'influence entre réalisateurs
st.header("Relations d'influence entre réalisateurs")
st.write(neo_queries.create_influence_relationships())

# 25. Chemin le plus court entre Tom Hanks et Scarlett Johansson
st.header("Chemin le plus court entre Tom Hanks et Scarlett Johansson")
st.write(neo_queries.shortest_path_between_actors("Tom Hanks", "Scarlett Johansson"))

# 26. Détection des communautés d'acteurs
st.header("Communautés d'acteurs")
st.write(neo_queries.detect_actor_communities())

# Fermer la connexion après usage
neo_queries.close()
