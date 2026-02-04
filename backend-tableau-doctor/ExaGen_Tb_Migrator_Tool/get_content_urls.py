"""
Helper script to find datasource content URLs
This helps you build the mapping file for workbook migration
"""

import logging
from dotenv import load_dotenv
from config import Config
from tableau_client import TableauClient
from datasource_manager import DatasourceManager
from workbook_manager import WorkbookManager
import xml.etree.ElementTree as ET
from pathlib import Path
import json

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_datasource_content_url(env: str, datasource_id: str) -> dict:
    """
    Get the content URL for a specific datasource
    
    Args:
        env: Environment (dev, test, prod)
        datasource_id: UUID of the datasource
        
    Returns:
        Dict with datasource info including content URL
    """
    config = Config.from_env(env)
    
    with TableauClient(config) as client:
        ds_manager = DatasourceManager(client)
        details = ds_manager.get_datasource_details(datasource_id)
        
        return {
            'id': details['id'],
            'name': details['name'],
            'contentUrl': details['contentUrl'],
            'project': details.get('project', {}).get('name', 'N/A')
        }


def list_all_datasources_with_content_urls(env: str):
    """
    List all datasources with their content URLs
    Useful for finding what you need to migrate
    """
    config = Config.from_env(env)
    
    with TableauClient(config) as client:
        ds_manager = DatasourceManager(client)
        datasources = ds_manager.list_datasources()
        
        print(f"\n{'='*80}")
        print(f"DATASOURCES IN {env.upper()} ENVIRONMENT")
        print(f"{'='*80}\n")
        
        results = []
        for i, ds in enumerate(datasources, 1):
            # Get full details including contentUrl
            details = ds_manager.get_datasource_details(ds['id'])
            
            info = {
                'name': details['name'],
                'id': details['id'],
                'contentUrl': details['contentUrl'],
                'project': ds.get('project', {}).get('name', 'N/A')
            }
            results.append(info)
            
            print(f"[{i}] {info['name']}")
            print(f"    ID: {info['id']}")
            print(f"    Content URL: {info['contentUrl']}")
            print(f"    Project: {info['project']}")
            print("-" * 80)
        
        return results


def extract_datasource_refs_from_workbook(workbook_path: Path) -> list:
    """
    Extract all datasource references from a workbook XML file
    This shows you which datasources the workbook uses
    
    Args:
        workbook_path: Path to .twb file
        
    Returns:
        List of datasource reference dictionaries
    """
    if not workbook_path.exists():
        raise FileNotFoundError(f"Workbook not found: {workbook_path}")
    
    tree = ET.parse(workbook_path)
    root = tree.getroot()
    
    def clean_tag(tag):
        return tag.split('}')[-1] if '}' in tag else tag
    
    datasource_refs = []
    
    # Find datasources element
    for child in root:
        if clean_tag(child.tag) == 'datasources':
            for ds in child:
                if clean_tag(ds.tag) == 'datasource':
                    ref = {'name': ds.get('name')}
                    
                    # Look for repository-location (published datasources)
                    for sub in ds:
                        if clean_tag(sub.tag) == 'repository-location':
                            path = sub.get('path', '')
                            ds_id = sub.get('id', '')
                            
                            # Extract content URL from path
                            # Path format: /t/sitename/datasources/contentUrl
                            if '/datasources/' in path:
                                content_url = path.split('/datasources/')[-1]
                                ref['contentUrl'] = content_url
                                ref['id'] = ds_id
                                ref['path'] = path
                        
                        # Get connection info
                        elif clean_tag(sub.tag) == 'connection':
                            ref['dbname'] = sub.get('dbname', '')
                            ref['type'] = sub.get('class', '')
                    
                    if 'contentUrl' in ref:  # Only add published datasources
                        datasource_refs.append(ref)
    
    return datasource_refs


def analyze_workbook_for_migration(env: str, workbook_id: str):
    """
    Download a workbook and analyze which datasources it uses
    This helps you build the mapping for migration
    """
    config = Config.from_env(env)
    output_dir = Path("./downloads")
    output_dir.mkdir(exist_ok=True)
    
    with TableauClient(config) as client:
        wb_manager = WorkbookManager(client)
        
        # Download workbook
        logger.info(f"Downloading workbook {workbook_id}...")
        file_path = wb_manager.download_workbook(workbook_id, output_dir)
        
        # Extract datasource references
        datasource_refs = extract_datasource_refs_from_workbook(file_path)
        
        print(f"\n{'='*80}")
        print(f"DATASOURCES USED BY WORKBOOK: {file_path.name}")
        print(f"{'='*80}\n")
        
        if not datasource_refs:
            print("No published datasources found in this workbook.")
            print("(It may use embedded or local datasources)")
            return []
        
        for i, ref in enumerate(datasource_refs, 1):
            print(f"[{i}] {ref.get('name', 'Unknown')}")
            print(f"    Content URL: {ref.get('contentUrl', 'N/A')}")
            print(f"    DB Name: {ref.get('dbname', 'N/A')}")
            print(f"    Type: {ref.get('type', 'N/A')}")
            print("-" * 80)
        
        return datasource_refs


def create_mapping_file(dev_datasources: list, prod_datasources: list, 
                       output_file: str = "datasource_mapping.json"):
    """
    Create a mapping file by matching datasources by name
    
    Args:
        dev_datasources: List of datasource dicts from dev (with contentUrl)
        prod_datasources: List of datasource dicts from prod (with contentUrl)
        output_file: Output JSON file path
    """
    # Create mapping by name
    mapping = {}
    
    for dev_ds in dev_datasources:
        dev_name = dev_ds['name']
        dev_content_url = dev_ds['contentUrl']
        
        # Find matching prod datasource by name
        prod_match = next((p for p in prod_datasources if p['name'] == dev_name), None)
        
        if prod_match:
            mapping[dev_content_url] = prod_match['contentUrl']
            logger.info(f"Mapped: {dev_name}")
            logger.info(f"  DEV:  {dev_content_url}")
            logger.info(f"  PROD: {prod_match['contentUrl']}")
        else:
            logger.warning(f"No match found in prod for: {dev_name}")
    
    # Save to file
    with open(output_file, 'w') as f:
        json.dump(mapping, f, indent=2)
    
    logger.info(f"\n✅ Mapping file created: {output_file}")
    return mapping


def interactive_mapping_wizard():
    """
    Interactive wizard to help create datasource mapping
    """
    print("\n" + "="*80)
    print("DATASOURCE MAPPING WIZARD")
    print("="*80 + "\n")
    
    # Step 1: Get workbook info
    print("Step 1: Analyze workbook in DEV")
    workbook_id = input("Enter DEV workbook ID: ").strip()
    
    if not workbook_id:
        print("No workbook ID provided. Exiting.")
        return
    
    print(f"\nAnalyzing workbook {workbook_id} in DEV...\n")
    dev_refs = analyze_workbook_for_migration('dev', workbook_id)
    
    if not dev_refs:
        print("\nNo datasources to map. Exiting.")
        return
    
    # Step 2: Get dev datasource details
    print("\n" + "="*80)
    print("Step 2: Getting DEV datasource details...")
    print("="*80 + "\n")
    
    dev_datasources = []
    for ref in dev_refs:
        content_url = ref.get('contentUrl')
        if content_url:
            # Try to find full details
            config = Config.from_env('dev')
            with TableauClient(config) as client:
                ds_manager = DatasourceManager(client)
                all_ds = ds_manager.list_datasources()
                
                # Find by content URL
                for ds in all_ds:
                    details = ds_manager.get_datasource_details(ds['id'])
                    if details['contentUrl'] == content_url:
                        dev_datasources.append(details)
                        print(f"Found: {details['name']} -> {content_url}")
                        break
    
    # Step 3: Get prod datasources
    print("\n" + "="*80)
    print("Step 3: Getting PROD datasources...")
    print("="*80 + "\n")
    
    prod_datasources = list_all_datasources_with_content_urls('prod')
    
    # Step 4: Create mapping
    print("\n" + "="*80)
    print("Step 4: Creating mapping file...")
    print("="*80 + "\n")
    
    mapping = create_mapping_file(dev_datasources, prod_datasources)
    
    print("\n✅ Mapping wizard complete!")
    print("You can now use datasource_mapping.json for migration.")


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python get_content_urls.py list <env>")
        print("  python get_content_urls.py get <env> <datasource_id>")
        print("  python get_content_urls.py analyze <env> <workbook_id>")
        print("  python get_content_urls.py wizard")
        print("\nExamples:")
        print("  python get_content_urls.py list dev")
        print("  python get_content_urls.py get dev 072ce65a-dbf9-405f-9a0e-31357101058e")
        print("  python get_content_urls.py analyze dev e622b5b0-08ec-4443-996d-0a9a158bf65c")
        print("  python get_content_urls.py wizard")
        sys.exit(1)
    
    command = sys.argv[1]
    
    try:
        if command == 'list':
            env = sys.argv[2] if len(sys.argv) > 2 else 'dev'
            list_all_datasources_with_content_urls(env)
            
        elif command == 'get':
            env = sys.argv[2] if len(sys.argv) > 2 else 'dev'
            ds_id = sys.argv[3] if len(sys.argv) > 3 else None
            if not ds_id:
                print("Error: datasource_id required")
                sys.exit(1)
            result = get_datasource_content_url(env, ds_id)
            print(f"\nDatasource: {result['name']}")
            print(f"Content URL: {result['contentUrl']}")
            
        elif command == 'analyze':
            env = sys.argv[2] if len(sys.argv) > 2 else 'dev'
            wb_id = sys.argv[3] if len(sys.argv) > 3 else None
            if not wb_id:
                print("Error: workbook_id required")
                sys.exit(1)
            analyze_workbook_for_migration(env, wb_id)
            
        elif command == 'wizard':
            interactive_mapping_wizard()
            
        else:
            print(f"Unknown command: {command}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)