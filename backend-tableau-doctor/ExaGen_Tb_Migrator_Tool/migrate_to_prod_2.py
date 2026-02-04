import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Dict, Tuple
import getpass


from config import Config
from tableau_client import TableauClient
from datasource_manager import DatasourceManager
from workbook_manager import WorkbookManager
from connection_manager import ConnectionManager
from urllib.parse import unquote_plus

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def migrate_multiple_datasources_and_workbook_interactive(
    dev_datasource_ids: List[str],
    dev_workbook_id: str,
    prod_project_id: str,
    datasource_db_configs: Dict[str, Dict[str, str]]
):
    """
    Migrate multiple datasources and a workbook from dev to prod.
    Automatically fetches datasource IDs and prompts for credentials interactively.

    Workflow:
    1. For each datasource ID:
       - Download from dev
       - Publish to prod (auto-fetch new ID)
       - PROMPT for production database credentials
       - Update connection with provided credentials
    2. Download workbook
    3. Update all datasource references
    4. Publish workbook to prod

    Args:
        dev_datasource_ids: List of datasource UUIDs from dev environment
        dev_workbook_id: Workbook UUID from dev
        prod_project_id: Target project UUID in prod

    Returns:
        Tuple of (workbook_url, workbook_name, datasource_mappings)

    Example:
        # Just provide the dev datasource IDs
        dev_datasource_ids = [
            'a5c0fe92-543d-4ee9-b4b8-9b5734896e1d',
            'b6d1gf03-654e-5ff0-c5c9-0c6845907f2e',
            'c7e2hg14-765f-6gg1-d6d0-1d7956018g3f'
        ]

        # The script will prompt you for credentials for each one
        url, name, mappings = migrate_multiple_datasources_and_workbook_interactive(
            dev_datasource_ids=dev_datasource_ids,
            dev_workbook_id='93d6b9c0-f9c1-49c4-9a0c-fbc5af98050c',
            prod_project_id='4ca27767-7e38-46b4-bf45-df8d9eed3a69'
        )
    """
    output_dir = Path("./downloads")
    output_dir.mkdir(exist_ok=True)

    datasource_mapping = {}
    datasource_info = []  # Store (name, credentials) for summary

    logger.info("=" * 80)
    logger.info(f"STARTING INTERACTIVE MULTI-DATASOURCE MIGRATION")
    logger.info(f"Total datasources: {len(dev_datasource_ids)}")
    logger.info("=" * 80)
    logger.info("\nYou will be prompted to enter credentials for each datasource.\n")

    # ========================================================================
    # PHASE 1: MIGRATE ALL DATASOURCES (Prompt for credentials)
    # ========================================================================

    dev_config = Config.from_env('dev')
    prod_config = Config.from_env('prod')

    for idx, dev_ds_id in enumerate(dev_datasource_ids, 1):
        logger.info("\n" + "=" * 80)
        logger.info(f"DATASOURCE {idx}/{len(dev_datasource_ids)}")
        logger.info("=" * 80)

        try:
            # Step 1: Download from Dev
            logger.info(f"\n--- Step 1.{idx}: Downloading Datasource from Dev ---")
            with TableauClient(dev_config) as dev_client:
                ds_manager = DatasourceManager(dev_client)
                file_path = ds_manager.download_datasource(dev_ds_id, output_dir)
                datasource_name = unquote_plus(file_path.stem)
                logger.info(f"✓ Downloaded: {file_path.name}")

                # Extract and print dev metadata
                params = ds_manager.get_local_connection_params(file_path)
                logger.info("\n" + "-" * 60)
                logger.info(f" DEV DATASOURCE INFO: {datasource_name}")
                logger.info(f"   ID:       {dev_ds_id}")
                logger.info(f"   Host:     {params.get('host', 'N/A')}")
                logger.info(f"   Port:     {params.get('port', 'N/A')}")
                logger.info(f"   DB Name:  {params.get('dbname', 'N/A')}")
                logger.info(f"   Username: {params.get('username', 'N/A')}")
                logger.info("-" * 60)

                # Get old content URL
                details = ds_manager.get_datasource_details(dev_ds_id)
                old_content_url = details['contentUrl']
                logger.info(f"   Content URL: {old_content_url}")

            # Step 2: Publish to Prod (AUTO-FETCH new datasource ID)
            logger.info(f"\n--- Step 2.{idx}: Publishing Datasource to Prod ---")
            with TableauClient(prod_config) as prod_client:
                ds_manager_prod = DatasourceManager(prod_client)

                result = ds_manager_prod.publish_datasource(
                    file_path=file_path,
                    datasource_name=datasource_name,
                    project_id=prod_project_id,
                    overwrite=True
                )

                # AUTO-FETCHED new datasource ID
                new_datasource_id = result['id']
                new_content_url = result['contentUrl']

                logger.info(f"✓ Published successfully!")
                logger.info(f"   New Datasource ID: {new_datasource_id}")
                logger.info(f"   Content URL: {new_content_url}")

                # Step 3: PROMPT for Production DB Credentials
                logger.info(f"\n--- Step 3.{idx}: Update Connection ---")

                # credentials will be dynamically passed here
                cfg = datasource_db_configs[dev_ds_id]

                conn_manager = ConnectionManager(prod_client)
                conn_manager.update_datasource_connection(
                    datasource_id=new_datasource_id,
                    server_address=cfg["host"],
                    server_port=cfg["port"],
                    username=cfg["username"],
                    password=cfg["password"]
                )

                logger.info(f"✓ Connection updated successfully!")
                logger.info(f"   Connected to: {cfg['host']}:{cfg['port']}")
                logger.info(f"   Using user: {cfg['username']}")

                # Store mapping and info for summary
                datasource_mapping[old_content_url] = new_content_url
                datasource_info.append((datasource_name, cfg))

                logger.info(f"\n Datasource {idx} migration complete!")
                logger.info(f"   Mapping: {old_content_url} → {new_content_url}")

        except Exception as e:
            logger.error(f" Failed to migrate datasource {dev_ds_id}: {e}", exc_info=True)
            raise

    # ========================================================================
    # PHASE 2: MIGRATE WORKBOOK
    # ========================================================================

    logger.info("\n\n" + "=" * 80)
    logger.info("PHASE 2: WORKBOOK MIGRATION")
    logger.info("=" * 80)

    try:
        # Step 4: Download Workbook from Dev
        logger.info("\n--- Step 4: Downloading Workbook from Dev ---")
        with TableauClient(dev_config) as dev_client:
            wb_manager = WorkbookManager(dev_client)
            wb_file_path = wb_manager.download_workbook(dev_workbook_id, output_dir)
            workbook_name = unquote_plus(wb_file_path.stem)
            logger.info(f"✓ Downloaded: {wb_file_path.name}")

        # Step 5: Update Datasource References
        logger.info("\n--- Step 5: Updating Datasource References in Workbook ---")
        logger.info(f"Applying {len(datasource_mapping)} datasource mappings:")
        for old_url, new_url in datasource_mapping.items():
            logger.info(f"  • {old_url} → {new_url}")

        with TableauClient(dev_config) as dev_client:
            wb_manager = WorkbookManager(dev_client)
            wb_manager.update_datasource_references(wb_file_path, datasource_mapping)

        logger.info("✓ References updated successfully!")

        # Step 6: Publish Workbook to Prod
        logger.info("\n--- Step 6: Publishing Workbook to Prod ---")
        with TableauClient(prod_config) as prod_client:
            wb_manager_prod = WorkbookManager(prod_client)

            result = wb_manager_prod.publish_workbook(
                file_path=wb_file_path,
                workbook_name=workbook_name,
                project_id=prod_project_id,
                overwrite=True
            )


            workbook_url = result['webpageUrl']
            workbook_name = workbook_name
            logger.info(f"✓ Workbook published successfully!")
            logger.info(f"   URL: {workbook_url}")

        # ========================================================================
        # FINAL SUMMARY
        # ========================================================================

        logger.info("\n\n" + "=" * 80)
        logger.info(" MIGRATION COMPLETE - SUMMARY")
        logger.info("=" * 80)

        logger.info(f"\n Migrated {len(dev_datasource_ids)} datasource(s):")
        for idx, (name, creds) in enumerate(datasource_info, 1):
            logger.info(f"\n  {idx}. {name}")
            logger.info(f"     → Server: {creds['server_address']}:{creds['server_port']}")
            logger.info(f"     → User:   {creds['username']}")

        logger.info(f"\n Migrated workbook: {workbook_name}")
        logger.info(f"   URL: {workbook_url}")

        logger.info("\n Datasource mappings applied:")
        for old_url, new_url in datasource_mapping.items():
            logger.info(f"   • {old_url} → {new_url}")

        logger.info("\n" + "=" * 80)
        logger.info(" ALL DONE! Your workbook is now live in production!")
        logger.info("=" * 80 + "\n")

        return workbook_url, workbook_name, datasource_mapping

    except Exception as e:
        logger.error(f" Workbook migration failed: {e}", exc_info=True)
        raise


from datetime import datetime

def full_migration(
    dev_datasource_ids: List[str],
    dev_workbook_id: str,
    prod_project_id: str,
    datasource_db_configs: Dict[str, Dict[str, str]],
    task_id: str,
    progress_store: dict
):
    """
    Entry point for FastAPI thread.
    Bridges API payload → existing migration logic.
    """

    logger.info(f"[API] Migration started for task {task_id}")

    if progress_store is not None and task_id in progress_store:
        progress_store[task_id]["stage"] = 10
        progress_store[task_id]["message"] = "Starting datasource migration"

    workbook_url, workbook_name, datasource_mapping = (
        migrate_multiple_datasources_and_workbook_interactive(
            dev_datasource_ids=dev_datasource_ids,
            dev_workbook_id=dev_workbook_id,
            prod_project_id=prod_project_id,
            datasource_db_configs=datasource_db_configs
        )
    )

    progress_store[task_id] = {
        "stage": 100,
        "status": "success",
        "message": "Migration completed successfully",
        "workbook_url": workbook_url,
        "timestamp": datetime.now().isoformat()

    }
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info(f"✅ MIGRATION COMPLETE: {workbook_name}")
    logger.info("=" * 60)
    logger.info(f"Datasource: {datasource_mapping}")
    logger.info(f"Workbook URL: {workbook_url}")
        

    return {
        "workbook_url": workbook_url,
        "workbook_name": workbook_name,
        "datasource_mapping": datasource_mapping
    }

def run_migration_from_api(
    dev_datasource_ids: List[str],
    dev_workbook_id: str,
    prod_project_id: str,
    datasource_db_configs: Dict[str, Dict[str, str]],
    task_id: str,
    progress_store: dict
):
    return full_migration(
        dev_datasource_ids=dev_datasource_ids,
        dev_workbook_id=dev_workbook_id,
        prod_project_id=prod_project_id,
        datasource_db_configs=datasource_db_configs,
        task_id=task_id,
        progress_store=progress_store
    )


if __name__ == '__main__':
     print("This module is designed to be triggered via FastAPI.")
    