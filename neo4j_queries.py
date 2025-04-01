from neo4j_connection import Neo4jConnection

class Neo4jQueries:
    def __init__(self):
        self.conn = Neo4jConnection()
        self.driver = self.conn.connect()
    
    def run_query(self, query, **params):
        with self.driver.session() as session:
            result = session.run(query, **params)
            return [dict(record) for record in result]
    
    # Question 14
    def get_most_prolific_actor(self):
        query = """
        MATCH (a:Actor)-[:ACTED_IN]->(f:Film)
        RETURN a.name AS actor_name, COUNT(f) AS film_count
        ORDER BY film_count DESC
        LIMIT 1
        """
        return self.run_query(query)
    
    # Question 15
    def get_actors_with_anne_hathaway(self):
        query = """
        MATCH (anne:Actor {name: 'Anne Hathaway'})-[:ACTED_IN]->(f:Film)<-[:ACTED_IN]-(coactor:Actor)
        WHERE coactor <> anne
        RETURN DISTINCT coactor.name AS actor_name
        """
        return self.run_query(query)
    
    # Question 16
    def get_actor_with_highest_total_revenue(self):
        query = """
        MATCH (a:Actor)-[:ACTED_IN]->(f:Film)
        RETURN a.name AS actor_name, SUM(f.revenue) AS total_revenue
        ORDER BY total_revenue DESC
        LIMIT 1
        """
        return self.run_query(query)
    
    # Question 17
    def get_average_votes(self):
        query = """
        MATCH (f:Film)
        RETURN avg(f.votes) AS average_votes
        """
        return self.run_query(query)
    
    # Question 18 (modifiée pour utiliser la propriété genre)
    def get_most_common_genre(self):
        query = """
        MATCH (f:Film)
        WHERE f.genre <> 'Unknown'
        WITH f.genre AS genre, COUNT(*) AS count
        RETURN genre, count
        ORDER BY count DESC
        LIMIT 1
        """
        return self.run_query(query)
    
    # Question 19
    def get_coactors_films(self, your_name="Matt Damon"):
        query = """
        MATCH (you:Actor {name: $your_name})-[:ACTED_IN]->(f:Film)<-[:ACTED_IN]-(coactor:Actor)-[:ACTED_IN]->(other_film:Film)
        WHERE NOT (you)-[:ACTED_IN]->(other_film)
        RETURN DISTINCT other_film.title AS film_title
        LIMIT 5
        """
        return self.run_query(query, your_name=your_name)
    
    # Question 20
    def get_director_most_actors(self):
        query = """
        MATCH (d:Director)-[:DIRECTED]->(f:Film)<-[:ACTED_IN]-(a:Actor)
        RETURN d.name AS director_name, COUNT(DISTINCT a) AS actor_count
        ORDER BY actor_count DESC
        LIMIT 1
        """
        return self.run_query(query)
    
    # Question 21
    def get_most_connected_films(self):
        query = """
        MATCH (f1:Film)<-[:ACTED_IN]-(a:Actor)-[:ACTED_IN]->(f2:Film)
        WHERE f1 <> f2
        RETURN f1.title AS film_title, COUNT(DISTINCT a) AS shared_actors_count
        ORDER BY shared_actors_count DESC
        LIMIT 5
        """
        return self.run_query(query)
    
    # Question 22
    def get_actors_most_directors(self):
        query = """
        MATCH (a:Actor)-[:ACTED_IN]->(f:Film)<-[:DIRECTED]-(d:Director)
        RETURN a.name AS actor_name, COUNT(DISTINCT d) AS director_count
        ORDER BY director_count DESC
        LIMIT 5
        """
        return self.run_query(query)
    
    # Question 23 (modifiée pour utiliser la propriété genre)
    def recommend_movie_for_actor(self, actor_name):
        query = """
        // Trouve les genres des films où l'acteur a joué
        MATCH (a:Actor {name: $actor_name})-[:ACTED_IN]->(f:Film)
        WITH COLLECT(DISTINCT f.genre) AS actor_genres
        
        // Trouve des films avec les mêmes genres que l'acteur n'a pas joué
        MATCH (recFilm:Film)
        WHERE recFilm.genre IN actor_genres
        AND NOT EXISTS { (a:Actor {name: $actor_name})-[:ACTED_IN]->(recFilm) }
        RETURN recFilm.title AS recommended_movie
        LIMIT 1
        """
        return self.run_query(query, actor_name=actor_name)
    
    # Question 24
    def create_influence_relationships(self):
        query = """
        MATCH (d1:Director)-[:DIRECTED]->()<-[:ACTED_IN]-(a:Actor)-[:ACTED_IN]->()<-[:DIRECTED]-(d2:Director)
        WHERE d1 <> d2 AND NOT (d1)-[:INFLUENCED_BY]->(d2)
        WITH d1, d2, COUNT(DISTINCT a) AS common_actors
        WHERE common_actors > 1
        MERGE (d1)-[:INFLUENCED_BY {weight: common_actors}]->(d2)
        RETURN COUNT(*) AS relationships_created
        """
        return self.run_query(query)
    
    # Question 25
    def shortest_path_between_actors(self, actor1, actor2):
        query = """
        MATCH path = shortestPath((a1:Actor {name: $actor1})-[:ACTED_IN|DIRECTED*]-(a2:Actor {name: $actor2}))
        RETURN [n IN nodes(path) | 
            CASE WHEN n:Actor THEN 'Actor: ' + n.name 
                 WHEN n:Film THEN 'Film: ' + n.title 
                 WHEN n:Director THEN 'Director: ' + n.name
                 ELSE '' END] AS path
        """
        return self.run_query(query, actor1=actor1, actor2=actor2)
    
    def close(self):
        self.conn.close()

    

if __name__ == "__main__":
    queries = Neo4jQueries()
    
    def print_result(title, result):
        print(f"\n=== {title} ===")
        if not result:
            print("Aucun résultat trouvé")
        else:
            for item in result:
                print(item)
    
    try:
        print_result("14. Acteur le plus prolifique", queries.get_most_prolific_actor())
        print_result("15. Acteurs avec Anne Hathaway", queries.get_actors_with_anne_hathaway())
        print_result("16. Acteur avec plus haut revenu", queries.get_actor_with_highest_total_revenue())
        print_result("17. Moyenne des votes", queries.get_average_votes())
        print_result("18. Genre le plus commun", queries.get_most_common_genre())
        print_result("19. Films des co-acteurs (Matt Damon)", queries.get_coactors_films("Matt Damon"))
        print_result("20. Réalisateur avec plus d'acteurs", queries.get_director_most_actors())
        print_result("21. Films les plus connectés", queries.get_most_connected_films())
        print_result("22. Acteurs avec plus de réalisateurs", queries.get_actors_most_directors())
        print_result("23. Recommandation pour Tom Hanks", queries.recommend_movie_for_actor("Tom Hanks"))
        
        # Nettoyer avant de créer
        queries.run_query("MATCH ()-[r:INFLUENCED_BY]->() DELETE r")
        print_result("24. Relations d'influence créées", queries.create_influence_relationships())
        
        print_result("25. Chemin Tom Hanks à Scarlett Johansson", 
                   queries.shortest_path_between_actors("Tom Hanks", "Scarlett Johansson"))
    finally:
        queries.close()
        
# from neo4j_connection import Neo4jConnection

# class Neo4jQueries:
#     def __init__(self):
#         self.conn = Neo4jConnection()
#         self.driver = self.conn.connect()
    
#     def run_query(self, query, **params):
#         with self.driver.session() as session:
#             result = session.run(query, **params)
#             return [dict(record) for record in result]
    
#     # Question 14
#     def get_most_prolific_actor(self):
#         query = """
#         MATCH (a:Actor)-[:A_JOUE_DANS]->(f:Film)
#         RETURN a.name AS actor_name, COUNT(f) AS film_count
#         ORDER BY film_count DESC
#         LIMIT 1
#         """
#         return self.run_query(query)
    
#     # Question 15
#     def get_actors_with_anne_hathaway(self):
#         query = """
#         MATCH (anne:Actor {name: 'Anne Hathaway'})-[:A_JOUE_DANS]->(f:Film)<-[:A_JOUE_DANS]-(coactor:Actor)
#         WHERE coactor <> anne AND coactor.name <> ""
#         RETURN DISTINCT coactor.name AS actor_name
#         """
#         return self.run_query(query)
    
#     # Question 16
#     def get_actor_with_highest_total_revenue(self):
#         query = """
#         MATCH (a:Actor)-[:A_JOUE_DANS]->(f:Film)
#         WHERE f.revenue IS NOT NULL AND f.revenue <> ""
#         WITH a, 
#             CASE 
#             WHEN toFloat(f.revenue) IS NOT NULL THEN toFloat(f.revenue) 
#             ELSE 0.0 
#             END AS revenue
#         RETURN a.name AS actor_name, SUM(revenue) AS total_revenue
#         ORDER BY total_revenue DESC
#         LIMIT 1
#         """
#         return self.run_query(query)
    
#     # Question 17
#     def get_average_votes(self):
#         query = """
#         MATCH (f:Film)
#         WHERE f.Votes IS NOT NULL AND f.Votes <> ""
#         WITH 
#             CASE 
#                 WHEN toInteger(f.Votes) IS NOT NULL THEN toInteger(f.Votes) 
#                 ELSE NULL 
#             END AS numeric_votes
#         WHERE numeric_votes IS NOT NULL
#         RETURN avg(numeric_votes) AS average_votes
#         """
#         return self.run_query(query)
    
#     # Question 18
#     def get_most_common_genre(self):
#         query = """
#         MATCH (f:Film)-[:EST_DU_GENRE]->(g:Genre)
#         RETURN g.name AS genre, COUNT(f) AS film_count
#         ORDER BY film_count DESC
#         LIMIT 1
#         """
#         return self.run_query(query)
    
#     # Question 19
#     def get_coactors_films(self, your_name="Matt Damon"):
#         query = """
#         MATCH (you:Actor {name: $your_name})-[:A_JOUE_DANS]->(f:Film)<-[:A_JOUE_DANS]-(coactor:Actor)-[:A_JOUE_DANS]->(other_film:Film)
#         WHERE NOT (you)-[:A_JOUE_DANS]->(other_film)
#         RETURN DISTINCT other_film.title AS film_title
#         """
#         return self.run_query(query, your_name=your_name)
    
#     # Question 20
#     def get_director_most_actors(self):
#         query = """
#         MATCH (d:Director)-[:A_REALISE]->(f:Film)<-[:A_JOUE_DANS]-(a:Actor)
#         RETURN d.name AS director_name, COUNT(DISTINCT a) AS actor_count
#         ORDER BY actor_count DESC
#         LIMIT 1
#         """
#         return self.run_query(query)
    
#     # Question 21
#     def get_most_connected_films(self):
#         query = """
#         MATCH (f1:Film)<-[:A_JOUE_DANS]-(a:Actor)-[:A_JOUE_DANS]->(f2:Film)
#         WHERE f1 <> f2 AND f1.title IS NOT NULL AND f2.title IS NOT NULL
#         RETURN f1.title AS film_title, COUNT(DISTINCT a) AS shared_actors_count
#         ORDER BY shared_actors_count DESC
#         LIMIT 5
#         """
#         return self.run_query(query)
    
#     # Question 22
#     def get_actors_most_directors(self):
#         query = """
#         MATCH (a:Actor)-[:A_JOUE_DANS]->(f:Film)<-[:A_REALISE]-(d:Director)
#         RETURN a.name AS actor_name, COUNT(DISTINCT d) AS director_count
#         ORDER BY director_count DESC
#         LIMIT 5
#         """
#         return self.run_query(query)
    
#     # Question 23
#     def recommend_movie_for_actor(self, actor_name):
#         query = """
#         MATCH (a:Actor {name: $actor_name})-[:A_JOUE_DANS]->(f:Film)
#         WITH a, f LIMIT 1  # Vérification que l'acteur existe
#         RETURN CASE 
#             WHEN a IS NULL THEN ['Acteur non trouvé']
#             ELSE ['Fonctionnalité à implémenter après correction des données'] 
#         END AS recommendation
#         """
#         return self.run_query(query, actor_name=actor_name)
    
#     # Question 24 (simplifiée)
#     def clean_influence_relationships(self):
#         query = """
#         MATCH ()-[r:INFLUENCE_PAR]->()
#         DELETE r
#         RETURN COUNT(r) AS deleted_relationships
#         """
#         return self.run_query(query)

#     def create_influence_relationships(self):
#         query = """
#         MATCH (d1:Director)-[:A_REALISE]->(f1:Film)-[:EST_DU_GENRE]->(g:Genre)<-[:EST_DU_GENRE]-(f2:Film)<-[:A_REALISE]-(d2:Director)
#         WHERE d1 <> d2 AND NOT (d1)-[:INFLUENCE_PAR]->(d2)
#         WITH d1, d2, COUNT(DISTINCT g) AS shared_genres
#         WHERE shared_genres > 2  // Seuil arbitraire
#         MERGE (d1)-[:INFLUENCE_PAR]->(d2)
#         RETURN COUNT(*) AS relationships_created
#         """
#         return self.run_query(query)
    
#     # Question 25
#     def shortest_path_between_actors(self, actor1, actor2):
#         query = """
#         MATCH path = shortestPath(
#             (a1:Actor {name: $actor1})-[:A_JOUE_DANS*]-(a2:Actor {name: $actor2})
#         )
#         RETURN [n IN nodes(path) | 
#             CASE WHEN n:Actor THEN 'Actor: ' + n.name 
#                  WHEN n:Film THEN 'Film: ' + n.title 
#                  ELSE '' END] AS path
#         """
#         return self.run_query(query, actor1=actor1, actor2=actor2)
    
#     # Question 26 (utilisant l'algorithme de Louvain)
#     def detect_actor_communities(self):
#         query = """
#         CALL gds.graph.project(
#             'actorNetwork',
#             ['Actor', 'Film'],
#             {
#                 A_JOUE_DANS: {
#                     orientation: 'UNDIRECTED'
#                 }
#             }
#         );
        
#         CALL gds.louvain.stream('actorNetwork')
#         YIELD nodeId, communityId
#         RETURN gds.util.asNode(nodeId).name AS actor_name, communityId
#         ORDER BY communityId, actor_name;
#         """
#         return self.run_query(query)

# if __name__ == "__main__":
#     queries = Neo4jQueries()
    
#     def print_result(title, result):
#         print(f"\n=== {title} ===")
#         for item in result:
#             print(item)
    
#     print_result("14. Acteur le plus prolifique", queries.get_most_prolific_actor())
#     print_result("15. Acteurs avec Anne Hathaway", queries.get_actors_with_anne_hathaway())
#     print_result("16. Acteur avec plus haut revenu", queries.get_actor_with_highest_total_revenue())
#     print_result("17. Moyenne des votes", queries.get_average_votes())
#     print_result("18. Genre le plus commun", queries.get_most_common_genre())
#     print_result("19. Films des co-acteurs (Matt Damon)", queries.get_coactors_films("Matt Damon"))
#     print_result("20. Réalisateur avec plus d'acteurs", queries.get_director_most_actors())
#     print_result("21. Films les plus connectés", queries.get_most_connected_films())
#     print_result("22. Acteurs avec plus de réalisateurs", queries.get_actors_most_directors())
#     print_result("23. Recommandation pour Tom Hanks", queries.recommend_movie_for_actor("Tom Hanks"))
#     queries.clean_influence_relationships()  # Nettoyer avant
#     queries.create_influence_relationships()  # Recréer proprement
#     print("24. Crées")
#     print_result("25. Chemin Tom Hanks à Scarlett Johansson", 
#                 queries.shortest_path_between_actors("Tom Hanks", "Scarlett Johansson"))