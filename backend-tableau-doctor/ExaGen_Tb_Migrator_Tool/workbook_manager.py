"""
Workbook management operations - CORRECT MULTIPART/MIXED VERSION
"""

import os
import re
import logging
from urllib import response
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Dict, Optional
# from tableau_client import TableauClient

from .tableau_client import TableauClient



logger = logging.getLogger(__name__)


class WorkbookManager:
    """Manage Tableau workbooks"""
    
    def __init__(self, client: TableauClient):
        self.client = client
    
    def list_workbooks(self) -> List[Dict]:
        """List all workbooks on the site"""
        endpoint = f"/sites/{self.client.site_id}/workbooks"
        response = self.client.get(endpoint)
        data = response.json()
        return data.get('workbooks', {}).get('workbook', [])
    
    def get_workbook_details(self, workbook_id: str) -> Dict:
        """Get detailed information about a workbook"""
        endpoint = f"/sites/{self.client.site_id}/workbooks/{workbook_id}"
        response = self.client.get(endpoint)
        data = response.json()
        return data['workbook']
    
    def download_workbook(self, workbook_id: str,
                         output_dir: Path = Path("./downloads"),
                         include_extract: bool = False) -> Path:
        """
        Download a workbook from Tableau Server
        
        Args:
            workbook_id: UUID of the workbook
            output_dir: Directory to save the file
            include_extract: Whether to include embedded extracts
            
        Returns:
            Path to the downloaded file
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        
        endpoint = f"/sites/{self.client.site_id}/workbooks/{workbook_id}/content"
        
        if not include_extract:
            endpoint += "?includeExtract=false"
        
        logger.info(f"Downloading workbook {workbook_id}...")
        
        # Stream the download
        response = self.client.get(endpoint, stream=True)
        
        # Extract filename from Content-Disposition header
        content_disp = response.headers.get('Content-Disposition', '')
        filename = "downloaded_workbook.twb"
        
        if content_disp:
            fname_match = re.findall(r'filename="?([^"]+)"?', content_disp)
            if fname_match:
                filename = fname_match[0]
        
        file_path = output_dir / filename
        
        # Write file in chunks
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        logger.info(f"✅ Workbook saved as: {file_path}")
        return file_path
    
    def publish_workbook(self, file_path: Path, workbook_name: str,
                        project_id: str, overwrite: bool = True) -> Dict:
        """
        Publish a workbook to Tableau Server
        Uses the EXACT format Tableau expects: multipart/mixed
        
        Args:
            file_path: Path to the .twb or .twbx file
            workbook_name: Name for the workbook
            project_id: Target project UUID
            overwrite: Whether to overwrite if exists
            
        Returns:
            Published workbook information
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        logger.info(f"Publishing workbook '{workbook_name}' to project {project_id}...")
        
        # Use a simple boundary (Tableau is very particular)
        boundary = "boundary_string"
        
        # Build the XML request - simple and clean
        xml_request = (
            '<tsRequest>\n'
            f'  <workbook name="{workbook_name}" showTabs="true">\n'
            f'    <project id="{project_id}"/>\n'
            '  </workbook>\n'
            '</tsRequest>'
        )
        
        # Part 1: XML payload
        xml_part = (
            f'--{boundary}\r\n'
            'Content-Disposition: name="request_payload"\r\n'
            'Content-Type: text/xml\r\n'
            '\r\n'
            f'{xml_request}\r\n'
        )
        
        # Part 2: File
        file_part_header = (
            f'--{boundary}\r\n'
            f'Content-Disposition: name="tableau_workbook"; filename="{file_path.name}"\r\n'
            'Content-Type: application/octet-stream\r\n'
            '\r\n'
        )
        
        # Read file
        with open(file_path, "rb") as f:
            file_content = f.read()
        
        # Build complete body
        body = (
            xml_part.encode('utf-8') +
            file_part_header.encode('utf-8') +
            file_content +
            f'\r\n--{boundary}--\r\n'.encode('utf-8')
        )
        
        # Endpoint with parameters
        endpoint = f"/sites/{self.client.site_id}/workbooks"
        params = []
        if overwrite:
            params.append("overwrite=true")
        params.append("skipConnectionCheck=true")
        
        if params:
            endpoint += "?" + "&".join(params)
        
        logger.info(f"Publishing to endpoint: {endpoint}")
        logger.info(f"File size: {len(file_content)} bytes")
        
        # Critical: Use multipart/mixed (not multipart/form-data!)
        headers = {
            "X-Tableau-Auth": self.client.token,
            "Content-Type": f"multipart/mixed; boundary={boundary}",
            "Accept": "application/json"
        }
        
        # Make the request
        import requests
        url = f"{self.client.config.server_url}/api/{self.client.config.api_version}{endpoint}"
        
        logger.info(f"Making POST request to: {url}")
        logger.info(f"Content-Type: {headers['Content-Type']}")
        
        try:
            response = requests.post(url, data=body, headers=headers)
            
            # Log response
            logger.info(f"Response status: {response.status_code}")
            
            if response.status_code == 403:
                logger.error("403 Forbidden - Permissions issue!")
                logger.error(f"Response: {response.text[:500]}")
            elif response.status_code == 406:
                logger.error("406 Not Acceptable - Content-Type issue!")
                logger.error(f"Response: {response.text[:500]}")
            elif response.status_code >= 400:
                logger.error(f"Error {response.status_code}")
                logger.error(f"Response: {response.text[:500]}")
                
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"✅ Workbook published successfully!")
            
            return result['workbook']
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Full error response: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
    
    def update_datasource_references(self, workbook_path: Path, 
                                     datasource_mapping: Dict[str, str]):
        """
        Update references using Triple-Check (Path, ID, or DBName)
        """
        logger.info(f"--- 🔍 STARTING ROBUST REFERENCE UPDATE ---")
        logger.info(f"Target file: {workbook_path}")
        logger.info(f"Mapping keys: {list(datasource_mapping.keys())}")
        
        tree = ET.parse(workbook_path)
        root = tree.getroot()
        
        def clean_tag(tag):
            return tag.split('}')[-1] if '}' in tag else tag
        
        changes = 0
        
        # Locate datasources wrapper
        datasources_tag = None
        for child in root:
            if clean_tag(child.tag) == 'datasources':
                datasources_tag = child
                break
        
        if datasources_tag is not None:
            for ds in datasources_tag:
                if clean_tag(ds.tag) == 'datasource':
                    
                    # 1. Gather all current identifiers for this datasource
                    repo_loc = None
                    current_path = ""
                    current_id = ""
                    current_dbname = ""
                    
                    # Get Repository Location info
                    for sub in ds:
                        if clean_tag(sub.tag) == 'repository-location':
                            repo_loc = sub
                            current_path = repo_loc.get('path', '')
                            current_id = repo_loc.get('id', '')
                            break
                    
                    # Get Connection info (to find dbname)
                    connection = None
                    for sub in ds:
                        if clean_tag(sub.tag) == 'connection':
                            connection = sub
                            break
                    # Check nested connection if needed
                    if connection is None:
                        for sub in ds:
                            if clean_tag(sub.tag) == 'connection' or 'named-connection' in sub.tag:
                                for deep_sub in sub:
                                    if clean_tag(deep_sub.tag) == 'connection':
                                        connection = deep_sub
                                        break
                                    if connection is not None: break
                    
                    if connection is not None:
                        current_dbname = connection.get('dbname', '')

                    logger.info(f"🔎 Scanning Datasource:")
                    logger.info(f"   Path:   '{current_path}'")
                    logger.info(f"   ID:     '{current_id}'")
                    logger.info(f"   DBName: '{current_dbname}'")

                    # 2. Check for ANY match in the mapping
                    matched_new_url = None
                    
                    for old_key, new_url in datasource_mapping.items():
                        # TRIPLE CHECK LOGIC
                        match_found = False
                        
                        if old_key in current_path: 
                            match_found = True
                            logger.info(f"   ✅ Matched on PATH")
                        elif old_key == current_id: 
                            match_found = True
                            logger.info(f"   ✅ Matched on ID")
                        elif old_key == current_dbname: 
                            match_found = True
                            logger.info(f"   ✅ Matched on DBNAME")
                            
                        if match_found:
                            matched_new_url = new_url
                            
                            # Perform Updates
                            if repo_loc is not None:
                                # Fix Path (preserve prefix)
                                if '/datasources/' in current_path:
                                    prefix = current_path.split('/datasources/')[0]
                                    new_path = f"{prefix}/datasources/{matched_new_url}"
                                    repo_loc.set('path', new_path)
                                else:
                                    # Fallback if path is weird - try to extract site from current path
                                    if '/t/' in current_path:
                                        site_part = current_path.split('/t/')[0]
                                        site_name = current_path.split('/t/')[1].split('/')[0] if '/t/' in current_path else ''
                                        repo_loc.set('path', f"{site_part}/t/{site_name}/datasources/{matched_new_url}")
                                    elif self.client and hasattr(self.client, 'site_id'):
                                        repo_loc.set('path', f"/t/{self.client.site_id}/datasources/{matched_new_url}")
                                    else:
                                        # Last resort - just update the datasource part
                                        repo_loc.set('path', f"/datasources/{matched_new_url}")
                                
                                repo_loc.set('id', matched_new_url)
                                
                            if connection is not None:
                                connection.set('dbname', matched_new_url)
                                # Sanitize
                                if connection.get('username'): connection.set('username', '')
                                if connection.get('password'): del connection.attrib['password']
                                
                            changes += 1
                            break # Stop checking keys for this datasource

        if changes > 0:
            tree.write(workbook_path, encoding='utf-8', xml_declaration=True)
            logger.info(f"✅ SUCCESS! Updated {changes} reference(s).")
        else:
            logger.warning("⚠️ No changes made. Please verify your JSON key matches one of the 'Scanning Datasource' values above.")
    
    def delete_workbook(self, workbook_id: str):
        """Delete a workbook"""
        endpoint = f"/sites/{self.client.site_id}/workbooks/{workbook_id}"
        self.client.delete(endpoint)
        logger.info(f"✅ Workbook {workbook_id} deleted")
        


