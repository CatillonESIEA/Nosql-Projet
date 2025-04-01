from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

class Neo4jConnection:
    def __init__(self):
        self.uri = os.getenv("NEO4J_URI")
        self.user = os.getenv("NEO4J_USER")
        self.password = os.getenv("NEO4J_PASSWORD")
        self.driver = None

    def connect(self):
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        return self.driver

    def close(self):
        if self.driver is not None:
            self.driver.close()

if __name__ == "__main__":
    conn = Neo4jConnection()
    driver = conn.connect()
    try:
        with driver.session() as session:
            result = session.run("RETURN 'Connexion sécurisée réussie à Neo4j' AS message")
            print(result.single()["message"])
    except Exception as e:
        print(f"Erreur de connexion: {e}")
    finally:
        conn.close()