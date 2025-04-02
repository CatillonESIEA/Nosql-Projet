from neo4j_connection import Neo4jConnection

class Neo4jQueries:
    def __init__(self):
        self.conn = Neo4jConnection()
        self.driver = self.conn.connect()
    
    def run_query(self, query, **params):
        with self.driver.session() as session:
            result = session.run(query, **params)
            return [dict(record) for record in result]
        
    def close(self):
        if self.driver:
            self.driver.close()
    
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
    
    # Question 18
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
    
    # Question 23
    def recommend_movie_for_actor(self, actor_name):
        query = """
        MATCH (a:Actor {name: $actor_name})-[:ACTED_IN]->(f:Film)
        WITH COLLECT(DISTINCT f.genre) AS actor_genres
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
    
    # Question 26
    def detect_actor_communities(self):
        query = """
        MATCH (a:Actor)
        WITH a, id(a) AS id
        CALL {
            WITH a
            MATCH (a)-[:ACTED_IN]->()<-[:ACTED_IN]-(other:Actor)
            RETURN other
        }
        WITH a, id, COLLECT(DISTINCT other) AS collaborators
        RETURN a.name AS actor_name, 
            SIZE(collaborators) AS collaboration_count,
            HEAD([c IN collaborators | c.name]) AS main_collaborator
        ORDER BY collaboration_count DESC
        LIMIT 20
        """
        return self.run_query(query)
