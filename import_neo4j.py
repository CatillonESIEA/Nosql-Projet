from neo4j_connection import Neo4jConnection
import json

def import_data_to_neo4j():
    conn = Neo4jConnection()
    driver = conn.connect()
    
    with open('films_data.json', 'r', encoding='utf-8') as f:
        films_data = json.load(f)
    
    with driver.session() as session:
        # Supprime tout d'abord
        session.run("MATCH (n) DETACH DELETE n")
        
        # 1. Import des films et genres (genre comme propriété)
        session.run("""
        UNWIND $films AS film
        MERGE (f:Film {id: film.film_id})
        SET f += {
            title: film.title,
            year: film.year,
            rating: film.rating,
            revenue: film.revenue,
            runtime: film.runtime,
            votes: film.votes,
            metascore: film.metascore,
            genre: film.genre
        }
        """, films=films_data)
        
        # 2. Import des réalisateurs
        session.run("""
        UNWIND $films AS film
        WITH film WHERE film.director <> 'Inconnu'
        MERGE (d:Director {name: film.director})
        MERGE (f:Film {id: film.film_id})
        MERGE (d)-[:DIRECTED]->(f)
        """, films=films_data)
            
        # 3. Import des acteurs
        session.run("""
        UNWIND $films AS film
        UNWIND film.actors AS actor_name
        WITH film, actor_name WHERE actor_name <> ''
        MERGE (a:Actor {name: actor_name})
        MERGE (f:Film {id: film.film_id})
        MERGE (a)-[:ACTED_IN]->(f)
        """, films=films_data)
    
    conn.close()
    print("Import terminé avec succès!")

if __name__ == "__main__":
    import_data_to_neo4j()

# from neo4j_connection import Neo4jConnection
# import json

# def import_data_to_neo4j():
#     conn = Neo4jConnection()
#     driver = conn.connect()
    
#     with open('films_data.json', 'r', encoding='utf-8') as f:
#         films_data = json.load(f)
    
#     with driver.session() as session:
#         # 1. Crée les nœuds Films
        
#         #session.run("""
#         #UNWIND $films AS film
#         #MERGE (f:Film {id: film.film_id})
#         #SET f.title = film.title,
#         #    f.year = film.year,
#         #    f.rating = film.rating,
#         #    f.revenue = film["Revenue (Millions)"],
#         #    f.runtime = film["Runtime (Minutes)"],
#         #    f.director = film.director
#         #""", films=films_data)

#         # Import des genres
#         session.run("""
#             UNWIND $films AS film
#             WITH film, SPLIT(film.Genre, ', ') AS genres
#             UNWIND genres AS genre_name
#             MERGE (g:Genre {name: TRIM(genre_name)})
#             MERGE (f:Film {id: film.film_id})
#             MERGE (f)-[:EST_DU_GENRE]->(g)
#         """, films=films_data)
        
#         # Import des réalisateurs
#         session.run("""
#             MATCH (f:Film)
#             WHERE f.director IS NOT NULL
#             MERGE (d:Director {name: f.director})
#             MERGE (d)-[:A_REALISE]->(f)
#             REMOVE f.director
#         """)
            
#         # 2. Crée les nœuds Acteurs et relations
#         for film in films_data:
#             actors = film.get("Actors", "").split(", ") if film.get("Actors") else []
#             for actor in actors:
#                 if actor.strip():
#                     session.run("""
#                         MERGE (a:Actor {name: $actor_name})
#                         MERGE (f:Film {id: $film_id})
#                         MERGE (a)-[:A_JOUE_DANS]->(f)
#                     """, 
#                     actor_name=actor.strip(), 
#                     film_id=film["film_id"])
    
#     conn.close()
#     print("Import terminé avec succès!")

# if __name__ == "__main__":
#     import_data_to_neo4j()
