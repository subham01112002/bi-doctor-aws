import tableauserverclient as TSC
from dotenv import load_dotenv
from pathlib import Path
from .config import Config

load_dotenv()

def get_datasource_connection_info(datasource_luid: str):
    """
    Get connection details for a specific datasource
    
    Args:
        datasource_luid: The LUID of the datasource
        
    Returns:
        dict: Connection information
    """
    env_type = 'dev'
    config = Config.from_env(env_type)
    
    # 3. Initialize TSC using your config object attributes
    # Note: site_content_url in your config maps to site_id/site_name in TSC
    auth = TSC.PersonalAccessTokenAuth(
        token_name=config.pat_name,
        personal_access_token=config.pat_secret,
        site_id=config.site_content_url
    )
    server = TSC.Server(config.server_url, use_server_version=True)
    
    with server.auth.sign_in(auth):
        # Fetch datasource details
        ds = server.datasources.get_by_id(datasource_luid)
        
        # Fill the connections list for this datasource
        server.datasources.populate_connections(ds)
        
        connections = []
        for conn in ds.connections:
            connections.append({
                "type": conn.connection_type,
                "host": conn.server_address,
                "port": conn.server_port,
                "username": conn.username,
            })
        
        return {
            "datasource_name": ds.name,
            "connections": connections
        }


# Keep the original script execution for testing
if __name__ == "__main__":
    # 4. Target Datasource ID (using an example from your migrate_to_prod.py)
    # DATASOURCE_ID = "43a1237d-3dc5-49ee-998b-6c8585fe8259"
    DATASOURCE_ID = "c8f1b5fe-db02-4737-bbee-18eb67158bbb"
    
    result = get_datasource_connection_info(DATASOURCE_ID)
    
    print(f"\n" + "="*60)
    print(f"CONNECTION DETAILS FOR: {result['datasource_name']}")
    print("="*60)
    
    for conn in result['connections']:
        print(
            # f"ID:       {conn.id}\n"
            f"Type:     {conn['type']}\n"
            f"Host:     {conn['host']}\n"
            f"Port:     {conn['port']}\n"
            # f"DB Name:  {getattr(conn, 'dbname', 'N/A')}\n"
            f"Username: {conn['username']}\n"
            + "-"*30
        )