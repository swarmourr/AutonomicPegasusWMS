import sqlite3
from neo4j import GraphDatabase
import logging
from typing import Dict, List, Optional

class SQLiteToNeo4j:
    def __init__(self, sqlite_db: str, neo4j_uri: str, neo4j_user: str, neo4j_password: str, neo4j_database: str):
        """
        Initialize the SQLite to Neo4j loader with connection details.
        """
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
        self.logger = logging.getLogger(__name__)
        
        try:
            # SQLite connection
            self.sqlite_conn = sqlite3.connect(sqlite_db)
            self.sqlite_cursor = self.sqlite_conn.cursor()
            
            # Neo4j connection
            self.neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
            self.neo4j_database = neo4j_database
            
            self.logger.info("Database connections established successfully")
        except Exception as e:
            self.logger.error(f"Connection error: {e}")
            raise

    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """
        Retrieve detailed schema information for a given table.
        
        :param table_name: Name of the table
        :return: List of column dictionaries with details
        """
        try:
            self.sqlite_cursor.execute(f"PRAGMA table_info({table_name});")
            columns = []
            for row in self.sqlite_cursor.fetchall():
                columns.append({
                    'name': row[1],
                    'type': row[2],
                    'notnull': row[3],
                    'default': row[4],
                    'primary_key': row[5]
                })
            return columns
        except Exception as e:
            self.logger.error(f"Error retrieving schema for table {table_name}: {e}")
            return []

    def get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
        """
        Retrieve foreign key information for a given table.
        
        :param table_name: Name of the table
        :return: List of foreign key dictionaries
        """
        try:
            self.sqlite_cursor.execute(f"PRAGMA foreign_key_list({table_name});")
            foreign_keys = []
            for row in self.sqlite_cursor.fetchall():
                foreign_keys.append({
                    'id': row[0],
                    'seq': row[1],
                    'table': row[2],
                    'from': row[3],
                    'to': row[4],
                    'on_update': row[5],
                    'on_delete': row[6]
                })
            return foreign_keys
        except Exception as e:
            self.logger.error(f"Error retrieving foreign keys for table {table_name}: {e}")
            return []

    def get_table_names(self) -> List[str]:
        """
        Retrieve all table names from the SQLite database.
        
        :return: List of table names
        """
        try:
            self.sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in self.sqlite_cursor.fetchall()]
            self.logger.info(f"Found {len(tables)} tables in the database")
            return tables
        except Exception as e:
            self.logger.error(f"Error retrieving table names: {e}")
            return []

    def load_table_to_neo4j(self, table_name: str):
        """
        Load table data from SQLite to Neo4j as nodes.
        
        :param table_name: Name of the table to load
        """
        try:
            # Fetch data and columns
            self.sqlite_cursor.execute(f"SELECT * FROM {table_name}")
            columns = [description[0] for description in self.sqlite_cursor.description]
            
            # Create Neo4j session
            with self.neo4j_driver.session(database=self.neo4j_database) as session:
                # Batch processing for efficiency
                batch_size = 1000
                rows = self.sqlite_cursor.fetchall()
                
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i+batch_size]
                    
                    # Prepare batch create cypher query
                    create_nodes_query = f"""
                    UNWIND $batch AS row
                    CREATE (n:{table_name.capitalize()} {{ 
                        {', '.join([f"{col}: row.{col}" for col in columns])}
                    }})
                    """
                    
                    # Convert batch to list of dictionaries
                    batch_data = [dict(zip(columns, row)) for row in batch]
                    
                    # Execute batch create
                    session.run(create_nodes_query, {'batch': batch_data})
            
            self.logger.info(f"Successfully loaded {len(rows)} nodes for table {table_name}")
        
        except Exception as e:
            self.logger.error(f"Error loading table {table_name}: {e}")

    def create_relationships(self):
        """
        Create relationships between nodes based on foreign key constraints.
        """
        tables = self.get_table_names()
        
        with self.neo4j_driver.session(database=self.neo4j_database) as session:
            for table in tables:
                foreign_keys = self.get_foreign_keys(table)
                
                for fk in foreign_keys:
                    try:
                        query = f"""
                        MATCH (a:{table.capitalize()}), 
                              (b:{fk['table'].capitalize()})
                        WHERE a.{fk['from']} = b.{fk['to']}
                        CREATE (a)-[:{table.upper()}_TO_{fk['table'].upper()}]->(b)
                        """
                        
                        result = session.run(query)
                        relationship_count = result.summary().counters.relationships_created
                        
                        self.logger.info(
                            f"Created {relationship_count} relationships "
                            f"from {table} to {fk['table']} via {fk['from']} -> {fk['to']}"
                        )
                    
                    except Exception as e:
                        self.logger.error(f"Error creating relationships for {table}: {e}")

    def migrate_all_data(self):
        """
        Migrate entire database from SQLite to Neo4j.
        """
        try:
            # Load all tables
            tables = self.get_table_names()
            for table in tables:
                self.load_table_to_neo4j(table)
            
            # Create relationships based on foreign key constraints
            self.create_relationships()
            
            self.logger.info("Database migration completed successfully")
        
        except Exception as e:
            self.logger.error(f"Migration failed: {e}")
        
        finally:
            self.close()

    def close(self):
        """Close all database connections."""
        try:
            if hasattr(self, 'sqlite_conn'):
                self.sqlite_conn.close()
            if hasattr(self, 'neo4j_driver'):
                self.neo4j_driver.close()
            self.logger.info("Database connections closed")
        except Exception as e:
            self.logger.error(f"Error closing connections: {e}")

def main():
    # Configuration
    SQLITE_DB = "/home/hsafri/.pegasus/workflow.db"
    NEO4J_URI = "bolt://44.203.148.165"
    NEO4J_USERNAME = "neo4j"
    NEO4J_PASSWORD = "deviation-coordinator-assembly"
    NEO4J_DATABASE = "neo4j"
    
    # Initialize and run migration
    loader = SQLiteToNeo4j(
        sqlite_db=SQLITE_DB, 
        neo4j_uri=NEO4J_URI, 
        neo4j_user=NEO4J_USERNAME, 
        neo4j_password=NEO4J_PASSWORD, 
        neo4j_database=NEO4J_DATABASE
    )
    
    try:
        # Migrate all data
        loader.migrate_all_data()
    except Exception as e:
        print(f"Migration failed: {e}")

if __name__ == "__main__":
    main()