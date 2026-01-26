from app.utils.neo4j_connector import neo4j_driver

class SnaGraphController:
    
    def add_interaction(self, user_data: dict, post_data: dict, interaction_type="COMMENTED"):
        query = """
        MERGE (u:User {username: $username})
        SET u.name = $name, u.followers = $followers
        
        MERGE (p:Post {id: $post_id})
        SET p.caption = $caption, p.timestamp = datetime()
        
        MERGE (u)-[r:INTERACTED {type: $type}]->(p)
        SET r.weight = 1, r.timestamp = datetime()
        
        RETURN u, p
        """
        try:
            with neo4j_driver.get_session() as session:
                session.run(query, 
                            username=user_data['username'], 
                            name=user_data.get('name', user_data['username']),
                            followers=user_data.get('followers_count', 0),
                            post_id=post_data['id'],
                            caption=post_data.get('caption', '')[:50],
                            type=interaction_type
                           )
            return True
        except Exception as e:
            print(f"Error insert graph: {e}")
            return False

    def get_top_active_users(self):
        query = """
        MATCH (u:User)-[r]->(p:Post)
        RETURN u.username as user, count(r) as total_interactions
        ORDER BY total_interactions DESC
        LIMIT 10
        """
        try:
            with neo4j_driver.get_session() as session:
                result = session.run(query)
                return [record.data() for record in result]
        except Exception as e:
            print(f"Error analysis graph: {e}")
            return []