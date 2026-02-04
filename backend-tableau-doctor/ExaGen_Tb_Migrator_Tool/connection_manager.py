"""
Database connection management for datasources
"""

import logging
from typing import List, Dict, Optional
# from tableau_client import TableauClient

from .tableau_client import TableauClient



logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manage datasource database connections"""
    
    def __init__(self, client: TableauClient):
        self.client = client
    
    def get_datasource_connections(self, datasource_id: str) -> List[Dict]:
        """
        Get all connections for a datasource
        
        Args:
            datasource_id: UUID of the datasource
            
        Returns:
            List of connection dictionaries
        """
        endpoint = f"/sites/{self.client.site_id}/datasources/{datasource_id}/connections"
        response = self.client.get(endpoint)
        data = response.json()
        
        # Handle different response formats
        connections = self._extract_connections(data)
        
        if not connections:
            logger.warning(f"No connections found for datasource {datasource_id}")
            return []
        
        return connections
    
    def _extract_connections(self, conn_data: Dict) -> Optional[List[Dict]]:
        """
        Robustly extract connections from API response
        Handles multiple response formats
        """
        # Format 1: Nested dict with 'connection' key
        if 'connections' in conn_data and isinstance(conn_data['connections'], dict):
            if 'connection' in conn_data['connections']:
                conn_obj = conn_data['connections']['connection']
                if not isinstance(conn_obj, list):
                    return [conn_obj]
                return conn_obj
        
        # Format 2: Direct list under 'connections'
        if 'connections' in conn_data and isinstance(conn_data['connections'], list):
            return conn_data['connections']
        
        # Format 3: Direct 'connection' key
        if 'connection' in conn_data:
            conn_obj = conn_data['connection']
            if not isinstance(conn_obj, list):
                return [conn_obj]
            return conn_obj
        
        return None
    
    def update_datasource_connection(self, datasource_id: str,
                                    server_address: str,
                                    server_port: str,
                                    username: str,
                                    password: str,
                                    connection_id: Optional[str] = None,
                                    embed_password: bool = True) -> Dict:
        """
        Update database connection details for a datasource
        
        Args:
            datasource_id: UUID of the datasource
            server_address: Database server hostname/IP
            server_port: Database server port
            username: Database username
            password: Database password
            connection_id: Specific connection ID (if None, updates first connection)
            embed_password: Whether to embed the password
            
        Returns:
            Updated connection information
        """
        # Get connection ID if not provided
        if not connection_id:
            connections = self.get_datasource_connections(datasource_id)
            if not connections:
                raise ValueError(f"No connections found for datasource {datasource_id}")
            connection_id = connections[0]['id']
            logger.info(f"Using first connection: {connection_id}")
        
        logger.info(f"Updating connection {connection_id} for datasource {datasource_id}...")
        logger.info(f"   New server: {server_address}:{server_port}")
        
        # Prepare update payload
        update_payload = {
            "connection": {
                "serverAddress": server_address,
                "serverPort": server_port,
                "userName": username,
                "password": password,
                "embedPassword": embed_password
            }
        }
        
        endpoint = (f"/sites/{self.client.site_id}/datasources/{datasource_id}/"
                   f"connections/{connection_id}")
        
        response = self.client.put(endpoint, json_data=update_payload)
        result = response.json()
        
        connection = result['connection']
        logger.info(f"✅ Connection updated successfully!")
        logger.info(f"   Address: {connection['serverAddress']}:{connection['serverPort']}")
        logger.info(f"   Username: {connection['userName']}")
        
        return connection
    
    def test_connection(self, datasource_id: str, connection_id: Optional[str] = None) -> bool:
        """
        Test if a datasource connection is valid
        Note: This is a basic check - Tableau may not provide a direct test endpoint
        """
        try:
            connections = self.get_datasource_connections(datasource_id)
            if connection_id:
                conn = next((c for c in connections if c['id'] == connection_id), None)
                return conn is not None
            return len(connections) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def update_multiple_datasources(self, datasource_ids: List[str],
                                   server_address: str,
                                   server_port: str,
                                   username: str,
                                   password: str) -> Dict[str, bool]:
        """
        Update connections for multiple datasources
        
        Returns:
            Dictionary mapping datasource IDs to success status
        """
        results = {}
        
        for ds_id in datasource_ids:
            try:
                self.update_datasource_connection(
                    datasource_id=ds_id,
                    server_address=server_address,
                    server_port=server_port,
                    username=username,
                    password=password
                )
                results[ds_id] = True
            except Exception as e:
                logger.error(f"Failed to update datasource {ds_id}: {e}")
                results[ds_id] = False
        
        success_count = sum(results.values())
        logger.info(f"Updated {success_count}/{len(datasource_ids)} datasources")
        
        return results