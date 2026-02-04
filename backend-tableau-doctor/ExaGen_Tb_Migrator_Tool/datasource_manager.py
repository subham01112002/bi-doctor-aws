"""
Datasource management operations
"""

import os
import re
import logging
from pathlib import Path
from typing import List, Dict, Optional
# from tableau_client import TableauClient

from .tableau_client import TableauClient



logger = logging.getLogger(__name__)


class DatasourceManager:
    """Manage Tableau datasources"""
    
    def __init__(self, client: TableauClient):
        self.client = client
    
    def list_datasources(self) -> List[Dict]:
        """List all datasources on the site"""
        endpoint = f"/sites/{self.client.site_id}/datasources"
        response = self.client.get(endpoint)
        data = response.json()
        return data.get('datasources', {}).get('datasource', [])
    
    def get_datasource_details(self, datasource_id: str) -> Dict:
        """Get detailed information about a datasource"""
        endpoint = f"/sites/{self.client.site_id}/datasources/{datasource_id}"
        response = self.client.get(endpoint)
        data = response.json()
        return data['datasource']
    
    def download_datasource(self, datasource_id: str, 
                           output_dir: Path = Path("./downloads")) -> Path:
        """
        Download a datasource from Tableau Server
        
        Args:
            datasource_id: UUID of the datasource
            output_dir: Directory to save the file
            
        Returns:
            Path to the downloaded file
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        
        endpoint = f"/sites/{self.client.site_id}/datasources/{datasource_id}/content"
        
        logger.info(f"Downloading datasource {datasource_id}...")
        
        # Stream the download
        response = self.client.get(endpoint, stream=True)
        
        # Extract filename from Content-Disposition header
        content_disp = response.headers.get('Content-Disposition', '')
        filename = "downloaded_datasource.tdsx"
        
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
        
        logger.info(f"✅ Datasource saved as: {file_path}")
        return file_path
    
    def publish_datasource(self, file_path: Path, datasource_name: str,
                          project_id: str, overwrite: bool = True) -> Dict:
        """
        Publish a datasource to Tableau Server
        
        Args:
            file_path: Path to the .tdsx or .tds file
            datasource_name: Name for the datasource
            project_id: Target project UUID
            overwrite: Whether to overwrite if exists
            
        Returns:
            Published datasource information
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        logger.info(f"Publishing datasource '{datasource_name}' to project {project_id}...")
        
        # Build multipart payload
        boundary = "boundary_string"
        
        xml_part = (
            f'--{boundary}\r\n'
            f'Content-Disposition: name="request_payload"\r\n'
            f'Content-Type: text/xml\r\n\r\n'
            f'<tsRequest>'
            f'<datasource name="{datasource_name}">'
            f'<project id="{project_id}"/>'
            f'</datasource>'
            f'</tsRequest>\r\n'
        )
        
        file_header = (
            f'--{boundary}\r\n'
            f'Content-Disposition: name="tableau_datasource"; '
            f'filename="{file_path.name}"\r\n'
            f'Content-Type: application/octet-stream\r\n\r\n'
        )
        
        # Read file content
        with open(file_path, "rb") as f:
            file_content = f.read()
        
        # Combine parts
        payload = (
            xml_part.encode('utf-8') +
            file_header.encode('utf-8') +
            file_content +
            f'\r\n--{boundary}--\r\n'.encode('utf-8')
        )
        
        # Prepare endpoint
        endpoint = f"/sites/{self.client.site_id}/datasources"
        if overwrite:
            endpoint += "?overwrite=true"
        
        # Custom headers for multipart
        headers = {
            "X-Tableau-Auth": self.client.token,
            "Content-Type": f"multipart/mixed; boundary={boundary}",
            "Accept": "application/json"
        }
        
        response = self.client.post(endpoint, data=payload, custom_headers=headers)
        
        result = response.json()
        logger.info(f"✅ Datasource published successfully!")
        
        return result['datasource']
    
    def get_datasource_content_url(self, datasource_id: str) -> str:
        """
        Get the actual content URL for a datasource
        This is the internal name used by Tableau
        """
        details = self.get_datasource_details(datasource_id)
        return details['contentUrl']
    
    def delete_datasource(self, datasource_id: str):
        """Delete a datasource"""
        endpoint = f"/sites/{self.client.site_id}/datasources/{datasource_id}"
        self.client.delete(endpoint)
        logger.info(f"✅ Datasource {datasource_id} deleted")