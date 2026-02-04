import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Dict, Tuple
import getpass


from .config import Config
from .tableau_client import TableauClient
from .datasource_manager import DatasourceManager
from .workbook_manager import WorkbookManager
from .connection_manager import ConnectionManager
from datetime import datetime
import time

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
    datasource_db_configs: Dict[str, Dict[str, str]],
    task_id: str = None,
    progress_store: dict = None
):
    """
    Migrate multiple datasources and a workbook from dev to prod.
    """
    output_dir = Path("./downloads")
    output_dir.mkdir(exist_ok=True)

    datasource_mapping = {}
    datasource_info = []
    total_datasources = len(dev_datasource_ids)

    logger.info("=" * 80)
    logger.info(f"STARTING INTERACTIVE MULTI-DATASOURCE MIGRATION")
    logger.info(f"Total datasources: {total_datasources}")
    logger.info("=" * 80)

    dev_config = Config.from_env('dev')
    prod_config = Config.from_env('prod')

    # PHASE 1: MIGRATE ALL DATASOURCES
    for idx, dev_ds_id in enumerate(dev_datasource_ids, 1):
        logger.info("\n" + "=" * 80)
        logger.info(f"DATASOURCE {idx}/{total_datasources}")
        logger.info("=" * 80)

        try:
            # Step 1: Download from Dev
            stage_base = 10 + ((idx - 1) * 10)
            
            # Initial progress update for this datasource
            if progress_store is not None and task_id in progress_store:
                progress_store[task_id].update({
                    "stage": stage_base,
                    "message": f"Starting datasource {idx} of {total_datasources}",
                    "status": "in_progress",
                    "total_datasources": total_datasources,
                    "current_datasource": idx,
                    "timestamp": datetime.now().isoformat()
                })
            
            logger.info(f"\n--- Step 1.{idx}: Downloading Datasource from Dev ---")
            with TableauClient(dev_config) as dev_client:
                ds_manager = DatasourceManager(dev_client)
                file_path = ds_manager.download_datasource(dev_ds_id, output_dir)
                datasource_name = unquote_plus(file_path.stem)
                logger.info(f"✓ Downloaded: {file_path.name}")
                details = ds_manager.get_datasource_details(dev_ds_id)
                old_content_url = details['contentUrl']
                
                # Update progress with datasource name
                if progress_store is not None and task_id in progress_store:
                    progress_store[task_id].update({
                        "stage": stage_base + 1,
                        "message": f"Downloading Datasource: {datasource_name} ({idx}/{total_datasources})",
                        "current_datasource": idx,
                        "total_datasources": total_datasources,
                        "current_datasource_name": datasource_name
                    })

            # Step 2: Publish to Prod
            if progress_store is not None and task_id in progress_store:
                progress_store[task_id].update({
                    "stage": stage_base + 4,
                    "message": f"Publishing Datasource: {datasource_name} ({idx}/{total_datasources})",
                    "current_datasource": idx,
                    "current_datasource_name": datasource_name
                })
            
            logger.info(f"\n--- Step 2.{idx}: Publishing Datasource to Prod ---")
            with TableauClient(prod_config) as prod_client:
                ds_manager_prod = DatasourceManager(prod_client)
                result = ds_manager_prod.publish_datasource(
                    file_path=file_path,
                    datasource_name=datasource_name,
                    project_id=prod_project_id,
                    overwrite=True
                )
                new_datasource_id = result['id']
                new_content_url = result['contentUrl']
                logger.info(f"✓ Published successfully!")

                # Step 3: Update Connection
                if progress_store is not None and task_id in progress_store:
                    progress_store[task_id].update({
                        "stage": stage_base + 7,
                        "message": f"Updating Connections: {datasource_name} ({idx}/{total_datasources})",
                        "current_datasource": idx,
                        "current_datasource_name": datasource_name
                    })
                
                logger.info(f"\n--- Step 3.{idx}: Update Connection ---")
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
                
                datasource_mapping[old_content_url] = new_content_url
                datasource_info.append((datasource_name, cfg))

        except Exception as e:
            logger.error(f"❌ Failed to migrate datasource {dev_ds_id}: {e}", exc_info=True)
            if progress_store is not None and task_id in progress_store:
                progress_store[task_id].update({
                    "stage": -1,
                    "message": f"Failed at datasource {idx}/{total_datasources}: {str(e)}",
                    "status": "failed"
                })
            raise

    # PHASE 2: MIGRATE WORKBOOK
    workbook_stage_base = 10 + (total_datasources * 10)
    
    logger.info("\n\n" + "=" * 80)
    logger.info("PHASE 2: WORKBOOK MIGRATION")
    logger.info("=" * 80)

    try:
        # Step 4: Download Workbook
        if progress_store is not None and task_id in progress_store:
            progress_store[task_id].update({
                "stage": workbook_stage_base + 1,
                "message": "Downloading Workbook"
            })
        
        logger.info("\n--- Step 4: Downloading Workbook from Dev ---")
        with TableauClient(dev_config) as dev_client:
            wb_manager = WorkbookManager(dev_client)
            wb_file_path = wb_manager.download_workbook(dev_workbook_id, output_dir)
            workbook_name = unquote_plus(wb_file_path.stem)
            logger.info(f"✓ Downloaded: {wb_file_path.name}")

        # Step 5: Update References
        if progress_store is not None and task_id in progress_store:
            progress_store[task_id].update({
                "stage": workbook_stage_base + 10,
                "message": "Updating References"
            })
        
        logger.info("\n--- Step 5: Updating Datasource References ---")
        with TableauClient(dev_config) as dev_client:
            wb_manager = WorkbookManager(dev_client)
            wb_manager.update_datasource_references(wb_file_path, datasource_mapping)
        logger.info("✓ References updated!")

        # Step 6: Publish Workbook
        if progress_store is not None and task_id in progress_store:
            progress_store[task_id].update({
                "stage": workbook_stage_base + 20,
                "message": "Publishing Workbook"
            })
        
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
            logger.info(f"✓ Workbook published!")

        # FINAL SUCCESS
        if progress_store is not None and task_id in progress_store:
            progress_store[task_id].update({
                "stage": 100,
                "status": "completed",
                "message": "Migration completed successfully",
                "workbook_url": workbook_url,
                "web_url": workbook_url,
                "timestamp": datetime.now().isoformat()
            })

        logger.info("\n" + "=" * 80)
        logger.info("✅ MIGRATION COMPLETE")
        logger.info("=" * 80)

        return workbook_url, workbook_name, datasource_mapping

    except Exception as e:
        logger.error(f"❌ Workbook migration failed: {e}", exc_info=True)
        if progress_store is not None and task_id in progress_store:
            progress_store[task_id].update({
                "stage": -1,
                "message": f"Workbook migration failed: {str(e)}",
                "status": "failed"
            })
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
            datasource_db_configs=datasource_db_configs,
            task_id=task_id,
            progress_store=progress_store
        )
    )

    progress_store[task_id] = {
        "stage": 100,
        "status": "completed",
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
    