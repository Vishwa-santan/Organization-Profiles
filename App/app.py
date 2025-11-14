import os
import time
import logging
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL = 'neo4j+ssc://06a4e5ca.databases.neo4j.io'
USER = 'neo4j'
PASSWORD = "02nTJ1LdUkeZsp24mwimXlZjZA9omoOifRTcQQnT84g"

class Neo4jConnect:
    def __init__(self):
        self.driver = GraphDatabase.driver(URL, auth=(USER, PASSWORD), notifications_disabled_categories=['UNRECOGNIZED'])

    def close(self):
        self.driver.close()

    def check_health(self):
        try:
            with self.driver.session() as session:
                session.run("RETURN 1")
                return True
        except Exception as e:
            logger.error(f"Health check error: {e}")
            return str(e)

    def query(self, query, parameters=None):
        try:
            with self.driver.session() as session:
                result = session.run(query, parameters or {})
                return [record.data() for record in result]
        except Exception as e:
            logger.error(f"Query error: {e}")
            return str(e)