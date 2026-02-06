import logging
from fastapi import FastAPI, HTTPException, logger
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Response, Cookie
from util.auth_clients.tableau_auth import TableauAuthClient
from util.query_clients.tableau_query_client import TableauQueryClient
from util.config_managers.tableau_reader import TableauConfigManager
from util.tableau_excel_generator import TableauExcellGenerator
from core.models.tableau_dropdown_loader_models import DropdownLoaderResponse
from core.models.tableau_workbook_models import WorkbooksResponse
from core.models.tableau_datasource_models import DatasourceMetadataResponse
from core.managers.tableau_data_manager import TableauDataManager
from core.managers.tableau_datasource_manager import TableauDatasourceDataManager
from pydantic import BaseModel
from typing import List, Optional
from ExaGen_Tb_Migrator_Tool.migrate_to_prod import run_migration_from_api
from sse_starlette.sse import EventSourceResponse
import asyncio
import json
import uuid
from datetime import datetime
import psycopg2
import mysql.connector
import threading
from util.s3_uploader import upload_excel_to_s3
import os


class LoginRequest(BaseModel):
    token_name: str
    token_value: str

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}
# Add this line after app initialization
progress_store = {}
#deployment_results = {} # Store final results of deployments (url)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def init_clients( token_name=None, token_value=None,tableau_token: str | None = None,site_id: str | None = None):
    config = TableauConfigManager()
    auth_client = TableauAuthClient(config=config, token_name=token_name, token_value=token_value,auth_token=tableau_token,site_id=site_id)
    query_client = TableauQueryClient(auth_client)
    return auth_client, query_client

# # 1) Test JWT generation
# @app.get("/auth/jwt")
# def generate_jwt():
#     return {"jwt": getJwt()}

# 2) Test Tableau login
@app.post("/bi/auth/login")
def login(req: LoginRequest,response: Response):
    try:
        auth_client, _ = init_clients(
            token_name=req.token_name,
            token_value=req.token_value
        )
        auth_token, site_id, username = auth_client.sign_in()
        # ✅ set secure cookie
        response.set_cookie(
            key="tableau_token",
            value=auth_token,
            httponly=True,     # JS cannot access
            secure=False,      # True in production (HTTPS)
            samesite="lax",
            max_age= 7200   # 2 hour
        )
        response.set_cookie("tableau_site_id", site_id, httponly=True, max_age=7200)
        response.set_cookie("tableau_token_name", req.token_name, httponly=True, max_age=7200)
        response.set_cookie("tableau_token_value", req.token_value, httponly=True, max_age=7200)
        response.set_cookie("username", username, httponly=False, max_age=7200)

        return {"site_id": site_id, "tableau_token": auth_token, "username": username}
    except Exception as e:
        raise HTTPException(500, str(e))
    
@app.get("/bi/auth/me")
def auth_me(tableau_token: str | None = Cookie(default=None)):
    if not tableau_token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"authenticated": True}
    
# 3) Test workbook dropdown loader
@app.get("/bi/tableau/projects")
def load_projects( tableau_token: str | None = Cookie(default=None),tableau_site_id: str | None = Cookie(default=None)):
    try:
        if not tableau_token or not tableau_site_id:
            raise HTTPException(401, "Not authenticated")

        auth_client, qc = init_clients(
            tableau_token=tableau_token,
            site_id=tableau_site_id
        )
        query = qc.query_loader()
        result = qc.send_request(query)
        workbooks = result["data"]["workbooks"]
        print("RAW METADATA RESPONSE:", result) 

        workbooks = result.get("data", {}).get("workbooks", [])
        print("WORKBOOK COUNT:", len(workbooks))
        project_map ={}
        for wb in workbooks:
            project_luid = wb.get("projectLuid")
            project_name = wb.get("projectName")
            projectvizporturl_id = wb.get("projectVizportalUrlId")

            if project_luid and project_name and projectvizporturl_id:
                project_map[project_luid] = {
                                                "project_name": project_name,
                                                "projectvizporturl_id": projectvizporturl_id
                                            }

        projects = [
            {
                "project_luid": pluid,
                "project_name": pdata["project_name"],
                "projectvizporturl_id": pdata["projectvizporturl_id"]
            }
            for pluid, pdata in project_map.items()
        ]
        return projects
    
    except Exception as e:
        raise HTTPException(500, str(e))
    

@app.get("/bi/tableau/workbooks")
def get_workbooks_for_project(
    project_luid: str,
    tableau_token: str | None = Cookie(default=None),
    tableau_site_id: str | None = Cookie(default=None),
):
    if not tableau_token or not tableau_site_id:
        raise HTTPException(401, "Not authenticated")

    auth_client, qc = init_clients(
        tableau_token=tableau_token,
        site_id=tableau_site_id
    )
    # Workbook query
    query = qc.query_loader()
    result = qc.send_request(query)
    workbooks = result.get("data", {}).get("workbooks", [])

    #  FILTER + DISTINCT for workbook details(based on projectLuid)
    seen = set()
    filtered_workbooks = []

    for wb in workbooks:
        if wb.get("projectLuid") != project_luid:
            continue

        luid = wb.get("luid")
        if not luid or luid in seen:
            continue

        seen.add(luid)
        
        datasources = []
        for ds in wb.get("upstreamDatasources", []):
            datasources.append({
                "luid": ds.get("luid"),
                "name": ds.get("name")
            })

        filtered_workbooks.append({
            "id": wb.get("id"),
            "luid": wb.get("luid"),
            "name": wb.get("name"),
            "datasources": datasources
        })

    return filtered_workbooks

@app.get("/bi/tableau/datasources")
def get_datasources_for_project(
    project_vizportal_url_id: str,
    tableau_token: str | None = Cookie(default=None),
    tableau_site_id: str | None = Cookie(default=None),
):
    if not tableau_token or not tableau_site_id:
        raise HTTPException(401, "Not authenticated")

    auth_client, qc = init_clients(
        tableau_token=tableau_token,
        site_id=tableau_site_id
    )
    # Datasource query
    datasource_query = qc.query_datasource()
    datasource_result = qc.send_request(datasource_query)

    # FILTER + DISTINCT for datasource details (based on projectVizportalUrlId)
    datasources = datasource_result.get("data", {}).get("publishedDatasources", [])
    ds_seen = set()
    filtered_datasources = []

    for ds in datasources:
        if ds.get("projectVizportalUrlId") != project_vizportal_url_id:
            continue
        ds_id = ds.get("id")
        if not ds_id or ds_id in ds_seen:
            continue

        ds_seen.add(ds_id)
        filtered_datasources.append({
            "id": ds.get("id"),
            "luid": ds.get("luid"),
            "name": ds.get("name")
        })

    return filtered_datasources

# Import your source_db function
from ExaGen_Tb_Migrator_Tool.source_db import get_datasource_connection_info
@app.get("/bi/tableau/datasource_connection")
def get_datasource_connection(
    datasource_luid: str,
    tableau_token: str | None = Cookie(default=None),
    tableau_site_id: str | None = Cookie(default=None),
):
    """Get connection details for a datasource"""
    try:
        if not tableau_token or not tableau_site_id:
            raise HTTPException(401, "Not authenticated")
        
        # Call the function with datasource_luid
        connection_info = get_datasource_connection_info(datasource_luid)
        # print("datasource INFO:", datasource_luid)
        return connection_info
    
    except Exception as e:
        raise HTTPException(500, str(e))

# # 4) Test full workbook metadata
class MetadataRequest(BaseModel):
    workbook_ids: list[str]
    workbook_luids: list[str]
    

@app.post("/bi/tableau/workbook_metadata")
def get_workbooks_metadata_for_project(req: MetadataRequest, tableau_token: str | None = Cookie(default=None),tableau_site_id: str | None = Cookie(default=None)):
    try:
        if not tableau_token or not tableau_site_id:
            raise HTTPException(401, "Not authenticated")

        auth_client, qc = init_clients(
            tableau_token=tableau_token,
            site_id=tableau_site_id
        )
        print("req.workbook_ids:", req.workbook_ids)
        print("req.workbook_luids:", req.workbook_luids)
        query = qc.query_workbook_metadata(req.workbook_ids)
        raw_metadata = qc.send_request(query)

        query_usage_stats = qc.get_usage_stats_wb(req.workbook_luids)

        #  Convert raw response → WorkbooksResponse model
        full_workbook_data = WorkbooksResponse.parse_obj(
            raw_metadata["data"]
        )

        #  Initialize DataManager (FLATTENING happens here)
        data_manager = TableauDataManager(full_workbook_data)

        flat_data_wb = data_manager.get_flat_wb_data()
        flat_data_embd, flat_data_query = data_manager.get_flat_embd_data()

        #  Fetch usage stats
        usage_stats_response = qc.get_usage_stats_wb(
            workbook_luids=req.workbook_luids
        )

        # #  Prepare Excel package (4 sheets)
        # package = [
        #     {
        #         "sheet_name": "Dashboard Details",
        #         "payload": flat_data_wb,
        #         'columns':[
        #             'Project ID',
        #             'Project',
        #             'Workbook ID',
        #             'Workbook',
        #             'Workbook Owner ID',
        #             'Workbook Owner Username',
        #             'Dashboard ID',
        #             'Dashboard',
        #             'Sheet ID',
        #             'Sheet',
        #             'Field ID',
        #             'Field',
        #             'Field Type',
        #             'Datasource ID',
        #             'Datasource',
        #             'Table Name',
        #             'Column Name',
        #             'Formula'
        #         ],
        #     },
        #     {
        #         "sheet_name": "Datasource Details",
        #         "payload": flat_data_embd,
        #         'columns':[
        #             'Project ID',
        #             'Project',
        #             'Workbook ID',
        #             'Workbook',
        #             'Datasource ID',
        #             'Datasource',
        #             'Table',
        #             'Column',
        #             'Used In Sheet',
        #             'Custom Query'
        #         ],
        #     },
        #     {
        #         "sheet_name": "Custom Query Details",
        #         "payload": flat_data_query,
        #         'columns':[
        #             'Project ID',
        #             'Project',
        #             'Workbook ID',
        #             'Workbook',
        #             'Custom Query ID',
        #             'Custom Query',
        #             'Query'
        #         ],
        #     },
        #     {
        #         "sheet_name": "Usage Statistics",
        #         "payload": usage_stats_response,
        #         'columns':[
        #             'Project ID',
        #             'project',
        #             'workbook ID',
        #             'Workbook',
        #             #'Data Source',
        #             'View ID',
        #             'View',
        #             'created_at',
        #             'updated_at',
        #             'Total Views'
        #         ],
        #     },
        # ]

        # #  Generate Excel
        # excel_generator = TableauExcellGenerator(package=package)
        # excel_generator.generate_spreadsheet()
        # excel_generator.format_excel()
        # write_summary_counts(excel_generator,data_manager)

        # return {
        #     "message": "Excel generated successfully",
        #     "file_path": excel_generator.file_path
        # }
        #return raw_metadata["data"]["workbooks"], query_usage_stats
        # Return both raw metadata and processed data for potential Excel generation
        return {
            "raw_workbooks": raw_metadata["data"]["workbooks"],
            "usage_stats": query_usage_stats,
            "processed_data": {
                "workbook_details": flat_data_wb,
                "datasource_details": flat_data_embd,
                "custom_query_details": flat_data_query,
                "usage_statistics": usage_stats_response,
                "workbook_counts": data_manager.get_workbook_counts(),
                "datasource_counts": data_manager.get_datasource_counts()
            }
        }
    except Exception as e:
        raise HTTPException(500, str(e))


class DsMetadataRequest(BaseModel):
    datasource_ids: List[str]
    datasource_luids: List[str]

@app.post("/bi/tableau/datasource_metadata")
def get_datasource_metadata_for_project(req: DsMetadataRequest, tableau_token: str | None = Cookie(default=None),tableau_site_id: str | None = Cookie(default=None)):
    try:
        if not tableau_token or not tableau_site_id:
            raise HTTPException(401, "Not authenticated")

        auth_client, qc = init_clients(
            tableau_token=tableau_token,
            site_id=tableau_site_id
        )
        print("REQUESTED DATASOURCE IDS:", req.datasource_ids)
        ds_query = qc.query_datasource_metadata(req.datasource_ids)
        raw_metadata_ds = qc.send_request(ds_query)

        #query_usage_stats = qc.get_usage_stats_wb(req.workbook_luids)

        #  Convert raw response → DatasourceMetadataResponse model
        full_datasource_data = DatasourceMetadataResponse.parse_obj(
            raw_metadata_ds["data"]
        )

        #  Initialize DataManager (FLATTENING happens here)
        ds_data_manager = TableauDatasourceDataManager(full_datasource_data)

        flat_datasource_details = ds_data_manager.get_flat_datasource_details()
        flat_datasource_custom_query = ds_data_manager.get_flat_ds_custom_queries()
        
        # print(get_flat_datasource_details)
        #  Fetch usage stats
        # usage_stats_response = qc.get_usage_stats_wb(
        #     workbook_luids=req.workbook_luids
        # )
        return {
            "raw_datasources": raw_metadata_ds["data"]["publishedDatasources"],
            "processed_data": {
                "datasource_details": flat_datasource_details,
                "custom_query_details": flat_datasource_custom_query,
            }
        # return raw_metadata_ds["data"]["datasources"]
        }
    except Exception as e:
        raise HTTPException(500, str(e))


class GenerateExcelRequest(BaseModel):
    workbook_data: dict
    datasource_data: dict

@app.post("/bi/tableau/generate_combined_excel")
def generate_combined_excel(req: GenerateExcelRequest, tableau_token: str | None = Cookie(default=None), tableau_site_id: str | None = Cookie(default=None)):
    """
    Generate a single Excel file combining workbook and datasource metadata.
    Expects processed data from both workbook_metadata and datasource_metadata endpoints.
    """
    try:
        if not tableau_token or not tableau_site_id:
            raise HTTPException(401, "Not authenticated")

        # Extract workbook processed data
        wb_processed = req.workbook_data.get("processed_data", {})
        ds_processed = req.datasource_data.get("processed_data", {})

        # ============ COMBINED EXCEL PACKAGE ============
        package = [
            {
                "sheet_name": "WB_Dashboard Details",
                "payload": wb_processed.get("workbook_details", []),
                'columns': [
                    'Project ID', 'Project', 'Workbook ID', 'Workbook',
                    'Workbook Owner ID', 'Workbook Owner Username',
                    'Dashboard ID', 'Dashboard', 'Sheet ID', 'Sheet',
                    'Field ID', 'Field', 'Field Type',
                    'Datasource ID', 'Datasource', 'Table Name',
                    'Column Name', 'Formula'
                    ],
            },
            {
                "sheet_name": "WB_Datasource Details",
                "payload": wb_processed.get("datasource_details", []),
                'columns': [
                    'WB_Project ID', 'WB_Project', 'Workbook ID', 'Workbook LUID', 'Workbook', 'WB_Created Date', 'WB_Updated Date', 'WB_Tags', 'Description',
                    'Datasource ID', 'Datasource', 'DS_Created Date', 'DS_Updated Date', 'DS_Project ID', 'DS_Project', 'DS_Tags', 'Contains Extract', 'DataSource Type',
                    'Field ID', 'Field Name', 'Field Type', 'Formula', 'Table', 'Column', 'Sheet ID', 'Sheet', 'Used In Sheet', 'Dashboard ID', 'Dashboard', 'Custom Query', 'Flag'
                ],
            },
            {
                "sheet_name": "WB_Custom Query Details",
                "payload": wb_processed.get("custom_query_details", []),
                'columns': [
                    'Project ID', 'Project', 'Workbook ID', 'Workbook',
                    'Custom Query ID', 'Custom Query', 'Query','Flag'
                ],
            },
            {
                "sheet_name": "WB_Usage Statistics",
                "payload": wb_processed.get("usage_statistics", []),
                'columns': [
                    'Project ID', 'project', 'workbook ID', 'Workbook',
                    'View ID', 'View', 'created_at', 'updated_at', 'Total Views'
                ],
            },
            {
                "sheet_name": "DS_Datasource Details",
                "payload": ds_processed.get("datasource_details", []),
                "columns": [
                    "DS_Project ID", "DS_Project", "Datasource ID", "Datasource", "DS_Created Date", "DS_Updated Date", "Contains Extract",
                    "DS_Tags", "DataSource Type", "Field ID", "Field Name", "Field Type", "Formula", "Column", "Table", "Sheet ID", "Sheet",
                    "Used In Sheet", "Dashboard ID", "Dashboard", "Workbook ID", "Workbook", "Workbook LUID", "WB_Created Date", "WB_Updated Date",
                    "WB_Tags", "Description", "WB_Project", "WB_Project ID", "Custom Query", "Flag"
                ],
            },
            {
                "sheet_name": "DS_Custom Query Details",
                "payload": ds_processed.get("custom_query_details", []),
                'columns': [
                    'Project ID', 'Project', 'Workbook ID', 'Workbook',
                    'Custom Query ID', 'Custom Query', 'Query',"Flag"
                ],
            },
        ]

        # Generate Excel
        excel_generator = TableauExcellGenerator(package=package)
        excel_generator.generate_spreadsheet()
        excel_generator.format_excel()
        
        # Generate summary sheet with combined counts
        workbook_counts = wb_processed.get("workbook_counts", [])
        datasource_counts = wb_processed.get("datasource_counts", [])
        
        write_summary_counts_from_data(
            excel_generator, 
            workbook_counts, 
            datasource_counts
        )
        s3_key = upload_excel_to_s3(
            local_file_path=excel_generator.file_path,
            bucket="tableau-doctor-output"
        )

        return {
            "message": "Combined Excel generated successfully"
            # "file_path": excel_generator.file_path
        }

    except Exception as e:
        raise HTTPException(500, str(e))
    

# 5) sign out
@app.post("/bi/auth/logout")
def logout(
    response: Response,
    tableau_token: str | None = Cookie(default=None),
    tableau_site_id: str | None = Cookie(default=None),
):
    try:
        if tableau_token and tableau_site_id:
            auth_client, _ = init_clients(
                tableau_token=tableau_token,
                site_id=tableau_site_id
            )
            # Sign out from Tableau server
            auth_client.sign_out()

        # Clear cookies (VERY IMPORTANT)
        response.delete_cookie("tableau_token")
        response.delete_cookie("tableau_site_id")
        response.delete_cookie("tableau_token_name")
        response.delete_cookie("tableau_token_value")
        response.delete_cookie("username")

        return {"message": "Logged out successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
class TestConnectionRequest(BaseModel):
    db_type: str
    host: str
    port: str
    dbname: str
    username: str
    password: str

# 6. Test DB Connection
@app.post("/bi/db/test-connection")
def test_db_connection(req: TestConnectionRequest):
    """
    Test database connection with provided credentials
    """
    try:
        logging.info(f"Testing {req.db_type} connection to {req.host}:{req.port}")
        
        # Validate required fields
        if not all([req.host, req.port, req.username, req.password]):
            raise HTTPException(status_code=400, detail="All fields are required")
        
        # Test connection based on database type
        if req.db_type == "PostgreSQL":
            conn = psycopg2.connect(
                host=req.host,
                port=req.port,
                user=req.username,
                password=req.password,
                database=req.dbname,
                sslmode="require",
                connect_timeout=10  # 10 second timeout
            )
            conn.close()
            
        elif req.db_type == "MySQL":
            conn = mysql.connector.connect(
                host=req.host,
                port=req.port,
                user=req.username,
                password=req.password,
                database=req.dbname,
                ssl_verify_cert=False,
                ssl_disabled=False,
                connection_timeout=10  # 10 second timeout
            )
            conn.close()
            
        elif req.db_type == "Redshift":
            # Redshift uses PostgreSQL driver
            conn = psycopg2.connect(
                host=req.host,
                port=req.port,
                user=req.username,
                password=req.password,
                sslmode="require",
                connect_timeout=10
            )
            conn.close()
            
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported database type: {req.db_type}")
        
        logging.info("Database connection successful")
        return {
            "status": "success",
            "message": f"Successfully connected to {req.db_type} database!"
        }
        
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL connection error: {e}")
        return {
            "status": "error",
            "message": f"PostgreSQL connection failed: {str(e)}"
        }
        
    except mysql.connector.Error as e:
        logging.error(f"MySQL connection error: {e}")
        return {
            "status": "error",
            "message": f"MySQL connection failed: {str(e)}"
        }
        
    except Exception as e:
        logging.error(f"Connection test failed: {e}")
        return {
            "status": "error",
            "message": f"Connection failed: {str(e)}"
        }
    
# class DBConfig(BaseModel):
#     db_type: str
#     host: str
#     dbname: str
#     port: str
#     username: str
#     password: str
# datasource-level config model
class DatasourceDBConfig(BaseModel):
    db_type: str
    host: str
    dbname: str
    port: str
    username: str
    password: str

# Wrap each datasource payload
class DatasourceDeployConfig(BaseModel):
    datasource_luid: str
    db_config: DatasourceDBConfig

# Update DeployRequest model
class DeployRequest(BaseModel):
    source_workbook_luid: str
    datasource_luids: List[str]
    target_project_luid: str
    datasources: List[DatasourceDeployConfig]

@app.post("/bi/deploy/full-migration")
async def deploy_full_migration(req: DeployRequest):
    try:
        # Generate unique task ID
        task_id = str(uuid.uuid4())
        
        # ✅ CHANGE 1: Initialize progress BEFORE thread starts
        progress_store[task_id] = {
            'stage': 0,
            'message': 'Initializing deployment...',
            'status': 'started',
            'timestamp': datetime.now().isoformat()
        }
        
        # ✅ CHANGE 2: Add verification logging
        print(f"✅ [DEPLOY] Task {task_id} initialized")
        print(f"✅ [DEPLOY] Current tasks: {list(progress_store.keys())}")
        
        # Run migration in background thread
        def run_with_result_capture():
            try:
                print(f"🔄 [THREAD] Starting migration for {task_id}")
                
                # result = run_migration_from_api(
                #     req.source_workbook_luid,
                #     req.datasource_luids[0],
                #     req.target_project_luid,
                #     req.db_config.dict(),
                #     task_id,
                #     progress_store
                # )
                datasource_db_configs = {
                    ds.datasource_luid: ds.db_config.dict()
                    for ds in req.datasources
                }

                result = run_migration_from_api(
                    req.datasource_luids,
                    req.source_workbook_luid,
                    req.target_project_luid,
                    datasource_db_configs,
                    task_id,
                    progress_store
                )

                                
                print(f"✅ [THREAD] Migration completed for {task_id}")
                
            except Exception as e:
                print(f"❌ [THREAD] Migration error for {task_id}: {e}")
                import traceback
                traceback.print_exc()
                
                # ✅ CHANGE 3: Update progress store with error
                if task_id in progress_store:
                    progress_store[task_id] = {
                        'stage': -1,
                        'message': f'Migration failed: {str(e)}',
                        'status': 'failed',
                        'timestamp': datetime.now().isoformat()
                    }
                
        thread = threading.Thread(target=run_with_result_capture)
        thread.daemon = True
        thread.start()
        
        # ✅ CHANGE 4: Small delay to ensure thread started
        #time.sleep(0.1)

        return {
            "status": "started",
            "task_id": task_id,
            "message": "Migration started successfully"
        }

    except Exception as e:
        print(f"❌ [DEPLOY] Failed to start: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "status": "failed",
            "error": str(e)
        }


async def progress_generator(task_id: str):
    """SSE generator for progress updates"""
    try:
        # ✅ CHANGE 5: Add detailed logging
        print(f"🔌 [SSE] Client connected for task: {task_id}")
        print(f"🔍 [SSE] Available tasks: {list(progress_store.keys())}")
        
        # Wait for task to be initialized
        timeout = 30
        waited = 0
        while task_id not in progress_store and waited < timeout:
            await asyncio.sleep(0.1)
            waited += 0.1
            
            # ✅ CHANGE 6: Log waiting status every 2 seconds
            if int(waited * 10) % 20 == 0:
                print(f"[SSE] Waiting for task {task_id}... ({waited:.1f}s)")
        
        if task_id not in progress_store:
            print(f"❌ [SSE] Task {task_id} NOT FOUND after {timeout}s")
            
            # ✅ CHANGE 7: Use event-based SSE format
            yield {
                "event": "error",
                "data": json.dumps({
                    "stage": -1,
                    "message": "Task not found - please try again",
                    "status": "failed",
                    "timestamp": datetime.now().isoformat()
                })
            }
            return
        
        print(f"✅ [SSE] Task {task_id} found!")
        
        last_stage = -1
        last_message = ""
        consecutive_same = 0
        iterations = 0
        max_iterations = 600  # 5 minutes max
        
        while iterations < max_iterations:
            iterations += 1
            
            if task_id in progress_store:
                progress = progress_store[task_id]
                current_stage = progress.get('stage', 0)
                current_message = progress.get('message', '')
                status = progress.get('status', 'in_progress')
                
                # Send update if stage OR message changed
                if current_stage != last_stage or current_message != last_message:
                    # ✅ CHANGE 8: Send keepalive ping to prevent timeout
                    yield {
                        "event": "ping",
                        "data": ""
                    }
                    
                    # ✅ CHANGE 9: Send progress with event type
                    yield {
                        "event": "progress",
                        "data": json.dumps(progress)
                    }
                    
                    last_stage = current_stage
                    last_message = current_message
                    consecutive_same = 0
                    
                    print(f"[SSE] Sent: Stage {current_stage} - {current_message}")
                else:
                    consecutive_same += 1
                    
                    # ✅ CHANGE 10: Send periodic keepalive (every 10 iterations of no change)
                    if consecutive_same % 10 == 0:
                        yield {
                            "event": "keepalive",
                            "data": json.dumps({"timestamp": datetime.now().isoformat()})
                        }
                
                # Check if completed or failed
                if status in ['completed', 'failed']:
                    print(f"🏁 [SSE] Task {task_id} finished with status: {status}")
                    
                    # ✅ CHANGE 11: Send completion with event type
                    yield {
                        "event": "complete",
                        "data": json.dumps(progress)
                    }
                    
                    await asyncio.sleep(2)
                    break
            else:
                print(f"⚠️ [SSE] Task {task_id} disappeared from store")
                break
            
            await asyncio.sleep(0.3)
        
        # ✅ CHANGE 12: Handle timeout
        if iterations >= max_iterations:
            print(f"⏱️ [SSE] Task {task_id} timed out")
            yield {
                "event": "error",
                "data": json.dumps({
                    "stage": -1,
                    "message": "Operation timed out",
                    "status": "failed",
                    "timestamp": datetime.now().isoformat()
                })
            }
            
    except Exception as e:
        print(f"❌ [SSE] Error for {task_id}: {e}")
        import traceback
        traceback.print_exc()
        
        yield {
            "event": "error",
            "data": json.dumps({
                "stage": -1,
                "message": f"SSE Error: {str(e)}",
                "status": "failed",
                "timestamp": datetime.now().isoformat()
            })
        }
    finally:
        # Cleanup
        await asyncio.sleep(3)
        if task_id in progress_store:
            print(f"🧹 [SSE] Cleaning up task {task_id}")
            del progress_store[task_id]


@app.get("/bi/deploy/progress/{task_id}")
async def stream_progress(task_id: str):
    """SSE endpoint for real-time progress"""
    print(f"🔌 [SSE] Connection request for task: {task_id}")
    
    # ✅ CHANGE 13: AWS-optimized headers
    return EventSourceResponse(
        progress_generator(task_id),
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",  # Changed
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",  # Added
            "Content-Type": "text/event-stream",  # Added
        },
        ping=15,  # ✅ CHANGE 14: Built-in keepalive every 15 seconds
    )


# ✅ CHANGE 15: Add health check endpoint for ALB
@app.get("/bi/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_tasks": len(progress_store)
    }


# ✅ CHANGE 16: Add debug endpoint to check task status
@app.get("/bi/deploy/status/{task_id}")
async def get_task_status(task_id: str):
    """Check if task exists in progress store"""
    if task_id in progress_store:
        return {
            "exists": True,
            "progress": progress_store[task_id]
        }
    else:
        return {
            "exists": False,
            "message": "Task not found",
            "available_tasks": list(progress_store.keys())
        }


# ✅ CHANGE 17: Add endpoint to list all active tasks (debugging)
@app.get("/bi/deploy/tasks")
async def list_active_tasks():
    """List all active deployment tasks"""
    return {
        "count": len(progress_store),
        "tasks": list(progress_store.keys()),
        "details": progress_store
    }

def write_summary_counts(excel_generator, data_manager):
    """
    Generate a combined summary sheet showing workbook-level counts
    for dashboards, sheets, fields, datasources, tables, and columns.
    
    Used when you have DataManager objects (backward compatible).
    
    Args:
        excel_generator: TableauExcellGenerator instance
        data_manager: Workbook data manager
    """
    try:
        # Retrieve both sets of data
        workbook_counts = data_manager.get_workbook_counts()
        datasource_counts = data_manager.get_datasource_counts()
        
        # Generate the summary using the internal function
        _generate_summary_sheet(excel_generator, workbook_counts, datasource_counts)
        
        print("Workbook Summary sheet written successfully.")

    except Exception as e:
        logging.critical(f"Critical Error generating summary sheet: {e}")
        raise e


def write_summary_counts_from_data(excel_generator, workbook_counts, datasource_counts):
    """
    Generate a combined summary sheet from pre-computed count data.
    
    Used when you already have the count dictionaries (e.g., from API responses).
    
    Args:
        excel_generator: TableauExcellGenerator instance
        workbook_counts: List of workbook count dictionaries
        datasource_counts: List of datasource count dictionaries
    """
    try:
        # Generate the summary using the internal function
        _generate_summary_sheet(excel_generator, workbook_counts, datasource_counts)
        
        print("Workbook Summary sheet written successfully from provided data.")

    except Exception as e:
        logging.critical(f"Critical Error generating summary sheet: {e}")
        raise e


def _generate_summary_sheet(excel_generator, workbook_counts, datasource_counts):
    """
    Internal function to generate the actual summary sheet.
    Shared by both write_summary_counts and write_summary_counts_from_data.
    
    Args:
        excel_generator: TableauExcellGenerator instance
        workbook_counts: List of workbook count dictionaries
        datasource_counts: List of datasource count dictionaries
    """
    # Merge the data into one combined summary list
    summary_data = []
    for wb in workbook_counts:
        workbook_name = wb.get("Workbook")

        summary_entry = {
            "Workbook": workbook_name,
            "Dashboards": wb.get("Dashboards", 0),
            "Sheets": wb.get("Sheets", 0),
            "Fields": wb.get("Fields", 0),
            "Field Types": wb.get("Field Types", 0),
            "Formula Fields": wb.get("Formula Fields", 0),
        }

        # Find matching datasource record
        ds_match = next((d for d in datasource_counts if d.get("Workbook") == workbook_name), None)
        if ds_match:
            summary_entry["Datasources"] = ds_match.get("Datasources", 0)
            summary_entry["Tables"] = ds_match.get("Tables", 0)
            summary_entry["Columns"] = ds_match.get("Columns", 0)
            summary_entry["Custom Queries"] = ds_match.get("Custom Queries", 0)
            summary_entry["Custom Columns"] = ds_match.get("Custom Columns", 0)
        else:
            summary_entry.update({
                "Datasources": 0,
                "Tables": 0,
                "Columns": 0,
                "Custom Queries": 0,
                "Custom Columns": 0
            })

        summary_data.append(summary_entry)

    # Define the columns for Excel
    summary_columns = [
        "Workbook",
        "Dashboards",
        "Sheets",
        "Fields",
        "Field Types",
        "Formula Fields",
        "Datasources",
        "Tables",
        "Columns",
        "Custom Queries",
        "Custom Columns"
    ]

    # Write the combined summary sheet
    excel_generator.generate_summary_sheet(unique_counts=summary_data, columns=summary_columns)


# Additional Tableau endpoints(using TSC library)
from project_workbook_list import TableauCloudClient


@app.get("/bi/tableau/projects_list")
def load_projects(
    tableau_token: str | None = Cookie(default=None),
    tableau_token_name: str | None = Cookie(default=None),
    tableau_token_value: str | None = Cookie(default=None)
):
    try:
        if not tableau_token or not tableau_token_name or not tableau_token_value:
            raise HTTPException(status_code=401, detail="Not authenticated")

        print(tableau_token, tableau_token_name)
        client = TableauCloudClient(
            token_name=tableau_token_name,
            token_value=tableau_token_value,
            config_path="config/tableau.yaml"
        )

        projects = client.list_all_projects()

        return [
            {
                "project_luid": project.id,
                "project_name": project.name
            }
            for project in projects
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/bi/tableau/sqlproxy-workbooks")
def load_sqlproxy_workbooks(
    project_luid: str,
    tableau_token: str | None = Cookie(default=None),
    tableau_token_name: str | None = Cookie(default=None),
    tableau_token_value: str | None = Cookie(default=None)
):
    try:
        if not tableau_token or not tableau_token_name or not tableau_token_value:
            raise HTTPException(status_code=401, detail="Not authenticated")

        client = TableauCloudClient(
            token_name=tableau_token_name,
            token_value=tableau_token_value,
            config_path="config/tableau.yaml"
        )

        return client.get_workbooks_with_sqlproxy_only(
            target_project_id=project_luid
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import jwt
import time
import uuid
from datetime import datetime, timedelta


# Tableau Connected App Configuration
TABLEAU_CONNECTED_APP_CLIENT_ID = "333b9ed3-0f3c-492a-9fa3-262c34cdbc00"
TABLEAU_CONNECTED_APP_SECRET = "hHTHdubZxJzvWKHTErxiTCB8TNVLWjBxBMx5B2NIs/0="
TABLEAU_CONNECTED_APP_SECRET_ID = "dfc4b32c-6624-4516-8fb3-457725a28d6a"
TABLEAU_SERVER_URL = "https://us-west-2b.online.tableau.com"
    
class TableauAuthResponse(BaseModel):
    jwt_token: str
    site_id: str
    dashboard_url: str

@app.post("/bi/auth/tableau-auth", response_model=TableauAuthResponse)
async def get_tableau_token(
    tableau_token: str | None = Cookie(default=None),
    tableau_site_id: str | None = Cookie(default=None),
    username: Optional[str] = Cookie(None)):
    if not username:
        raise HTTPException(status_code=401, detail="Username cookie not found. Please login first.")
    #auth_jwt_token, site_id = auth_client.jwt_sign_in()
    logging.info(f"Generating Tableau JWT token for user: {username}")
    try:
        # Create JWT token for Tableau
        token_payload = {
            "iss": TABLEAU_CONNECTED_APP_CLIENT_ID,
            "exp": int(time.time()) + (5 * 60),  # Token expires in 20 minutes
            "jti": str(uuid.uuid4()),  # Unique token ID
            "aud": "tableau",
            "sub": username,  # Tableau username
            "scp": ["tableau:auth:signin",
                "tableau:content:read",
                "tableau:content:read",
                "tableau:workbooks:read",
                "tableau:views:read",
                "tableau:datasources:read","tableau:views:embed"]
        }
        
        # Sign the token with secret
        encoded_token = jwt.encode(
            token_payload,
            TABLEAU_CONNECTED_APP_SECRET,
            algorithm="HS256",
            headers={
                "kid": TABLEAU_CONNECTED_APP_SECRET_ID,
                "iss": TABLEAU_CONNECTED_APP_CLIENT_ID
            }
        )
        logging.info(f"Generated JWT token: {encoded_token}")
        auth_client, _ = init_clients(
            token_name=tableau_token,
            token_value=tableau_site_id
        )
        auth_jwt_token, site_id = auth_client.jwt_sign_in(encoded_token)
        
        dashboard_url = f"{TABLEAU_SERVER_URL}/t/exavalu/views/WorkbookSummary_17683044081920/WorkbookSummary"
        
        return TableauAuthResponse(
            jwt_token=encoded_token,
            site_id=site_id,
            dashboard_url=dashboard_url
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Token generation failed: {str(e)}")

