"""
Tableau Migration Tool - Main Entry Point
Handles migration of datasources and workbooks between Tableau environments
"""

import argparse
import logging
from pathlib import Path
from typing import Optional

from config import Config
from tableau_client import TableauClient
from datasource_manager import DatasourceManager
from workbook_manager import WorkbookManager
from connection_manager import ConnectionManager

from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def list_resources(client: TableauClient, resource_type: str):
    """List all datasources or workbooks"""
    logger.info(f"Fetching {resource_type}...")
    
    if resource_type == 'datasources':
        ds_manager = DatasourceManager(client)
        datasources = ds_manager.list_datasources()
        
        print(f"\n📊 Your Published Data Sources ({len(datasources)}):\n")
        for i, ds in enumerate(datasources, 1):
            print(f"[{i}]")
            print(f"Name: {ds['name']}")
            print(f"ID: {ds['id']}")
            print(f"Project: {ds.get('project', {}).get('name', 'N/A')}")
            print("-" * 60)
            
    elif resource_type == 'workbooks':
        wb_manager = WorkbookManager(client)
        workbooks = wb_manager.list_workbooks()
        
        print(f"\n📊 Your Published Workbooks ({len(workbooks)}):\n")
        for i, wb in enumerate(workbooks, 1):
            print(f"[{i}]")
            print(f"Name: {wb['name']}")
            print(f"ID: {wb['id']}")
            print(f"Project: {wb.get('project', {}).get('name', 'N/A')}")
            print("-" * 60)


def list_projects(client: TableauClient):
    """List all projects"""
    logger.info("Fetching projects...")
    projects = client.get_projects()
    
    print(f"\n📁 Projects Found ({len(projects)}):\n")
    for project in projects:
        print(f"- {project['name']} (ID: {project['id']})")


def migrate_datasource(
    client: TableauClient,
    datasource_id: str,
    target_project_id: str,
    output_dir: Path = Path("./downloads")
):
    """Download and republish a datasource"""
    ds_manager = DatasourceManager(client)
    
    # Download
    logger.info(f"Downloading datasource {datasource_id}...")
    file_path = ds_manager.download_datasource(datasource_id, output_dir)
    
    # Republish
    logger.info(f"Publishing to project {target_project_id}...")
    result = ds_manager.publish_datasource(
        file_path=file_path,
        datasource_name=file_path.stem,
        project_id=target_project_id,
        overwrite=True
    )
    
    logger.info(f"✅ Datasource published successfully!")
    logger.info(f"   Content URL: {result['contentUrl']}")
    logger.info(f"   Web URL: {result['webpageUrl']}")
    
    return result


def migrate_workbook(
    client: TableauClient,
    workbook_id: str,
    target_project_id: str,
    datasource_mapping: Optional[dict] = None,
    output_dir: Path = Path("./downloads")
):
    """Download, update datasource references, and republish a workbook"""
    wb_manager = WorkbookManager(client)
    
    # Download
    logger.info(f"Downloading workbook {workbook_id}...")
    file_path = wb_manager.download_workbook(workbook_id, output_dir)
    
    # Update datasource references if mapping provided
    if datasource_mapping:
        logger.info("Updating datasource references...")
        wb_manager.update_datasource_references(file_path, datasource_mapping)
    
    # Republish
    logger.info(f"Publishing to project {target_project_id}...")
    result = wb_manager.publish_workbook(
        file_path=file_path,
        workbook_name=file_path.stem,
        project_id=target_project_id,
        overwrite=True
    )
    
    logger.info(f"✅ Workbook published successfully!")
    logger.info(f"   Content URL: {result['contentUrl']}")
    logger.info(f"   Web URL: {result['webpageUrl']}")
    
    return result


def update_datasource_connection(
    client: TableauClient,
    datasource_id: str,
    new_host: str,
    new_port: str,
    new_username: str,
    new_password: str
):
    """Update the database connection for a datasource"""
    conn_manager = ConnectionManager(client)
    
    logger.info(f"Updating connection for datasource {datasource_id}...")
    result = conn_manager.update_datasource_connection(
        datasource_id=datasource_id,
        server_address=new_host,
        server_port=new_port,
        username=new_username,
        password=new_password
    )
    
    logger.info(f"✅ Connection updated successfully!")
    logger.info(f"   New Address: {result['serverAddress']}:{result['serverPort']}")
    
    return result


def main():
    parser = argparse.ArgumentParser(description='Tableau Migration Tool')
    parser.add_argument('--env', choices=['dev', 'test', 'prod'], required=True,
                       help='Environment to connect to')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # List commands
    list_parser = subparsers.add_parser('list', help='List resources')
    list_parser.add_argument('resource', choices=['datasources', 'workbooks', 'projects'])
    
    # Migrate datasource
    ds_migrate_parser = subparsers.add_parser('migrate-datasource', 
                                              help='Migrate a datasource')
    ds_migrate_parser.add_argument('--id', required=True, help='Datasource ID')
    ds_migrate_parser.add_argument('--project-id', required=True, 
                                   help='Target project ID')
    
    # Migrate workbook
    wb_migrate_parser = subparsers.add_parser('migrate-workbook', 
                                              help='Migrate a workbook')
    wb_migrate_parser.add_argument('--id', required=True, help='Workbook ID')
    wb_migrate_parser.add_argument('--project-id', required=True, 
                                   help='Target project ID')
    wb_migrate_parser.add_argument('--ds-mapping', 
                                   help='Datasource mapping JSON file')
    
    # Update connection
    conn_parser = subparsers.add_parser('update-connection', 
                                       help='Update datasource connection')
    conn_parser.add_argument('--id', required=True, help='Datasource ID')
    conn_parser.add_argument('--host', required=True, help='Database host')
    conn_parser.add_argument('--port', required=True, help='Database port')
    conn_parser.add_argument('--username', required=True, help='Database username')
    conn_parser.add_argument('--password', required=True, help='Database password')
    
    args = parser.parse_args()
    
    # Load configuration
    config = Config.from_env(args.env)
    client = TableauClient(config)
    
    try:
        if args.command == 'list':
            if args.resource == 'projects':
                list_projects(client)
            else:
                list_resources(client, args.resource)
                
        elif args.command == 'migrate-datasource':
            migrate_datasource(client, args.id, args.project_id)
            
        elif args.command == 'migrate-workbook':
            ds_mapping = None
            if args.ds_mapping:
                import json
                with open(args.ds_mapping) as f:
                    ds_mapping = json.load(f)
            migrate_workbook(client, args.id, args.project_id, ds_mapping)
            
        elif args.command == 'update-connection':
            update_datasource_connection(
                client, args.id, args.host, args.port, 
                args.username, args.password
            )
        else:
            parser.print_help()
            
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()