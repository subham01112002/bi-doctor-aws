"""
Base Tableau Server API Client
"""

import requests
import logging
from typing import Dict, Any, Optional
# from config import Config

from .config import Config



logger = logging.getLogger(__name__)


class TableauClient:
    """Base client for Tableau Server REST API"""
    
    def __init__(self, config: Config):
        self.config = config
        self.token: Optional[str] = None
        self.site_id: Optional[str] = None
        self.user_id: Optional[str] = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with Tableau Server using PAT"""
        auth_url = f"{self.config.server_url}/api/{self.config.api_version}/auth/signin"
        
        auth_payload = {
            "credentials": {
                "personalAccessTokenName": self.config.pat_name,
                "personalAccessTokenSecret": self.config.pat_secret,
                "site": {"contentUrl": self.config.site_content_url}
            }
        }
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        logger.info(f"Authenticating to {self.config.server_url}...")
        
        response = requests.post(auth_url, json=auth_payload, headers=headers)
        response.raise_for_status()
        
        auth_data = response.json()
        self.token = auth_data["credentials"]["token"]
        self.site_id = auth_data["credentials"]["site"]["id"]
        self.user_id = auth_data["credentials"]["user"]["id"]
        
        logger.info("✅ Authentication successful!")
        logger.info(f"   Site ID: {self.site_id}")
    
    def _get_headers(self, accept_json: bool = True) -> Dict[str, str]:
        """Get headers for authenticated requests"""
        headers = {"X-Tableau-Auth": self.token}
        if accept_json:
            headers["Accept"] = "application/json"
        return headers
    
    def get(self, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated GET request"""
        url = f"{self.config.server_url}/api/{self.config.api_version}{endpoint}"
        headers = self._get_headers()
        
        response = requests.get(url, headers=headers, **kwargs)
        response.raise_for_status()
        return response
    
    def post(self, endpoint: str, json_data: Optional[Dict] = None, 
             data: Optional[bytes] = None, custom_headers: Optional[Dict] = None,
             **kwargs) -> requests.Response:
        """Make authenticated POST request"""
        url = f"{self.config.server_url}/api/{self.config.api_version}{endpoint}"
        
        if custom_headers:
            headers = custom_headers
            headers["X-Tableau-Auth"] = self.token
        else:
            headers = self._get_headers()
            headers["Content-Type"] = "application/json"
        
        if json_data:
            response = requests.post(url, json=json_data, headers=headers, **kwargs)
        elif data:
            response = requests.post(url, data=data, headers=headers, **kwargs)
        else:
            response = requests.post(url, headers=headers, **kwargs)
        
        response.raise_for_status()
        return response
    
    def put(self, endpoint: str, json_data: Dict, **kwargs) -> requests.Response:
        """Make authenticated PUT request"""
        url = f"{self.config.server_url}/api/{self.config.api_version}{endpoint}"
        headers = self._get_headers()
        headers["Content-Type"] = "application/json"
        
        response = requests.put(url, json=json_data, headers=headers, **kwargs)
        response.raise_for_status()
        return response
    
    def delete(self, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated DELETE request"""
        url = f"{self.config.server_url}/api/{self.config.api_version}{endpoint}"
        headers = self._get_headers()
        
        response = requests.delete(url, headers=headers, **kwargs)
        response.raise_for_status()
        return response
    
    def get_projects(self) -> list:
        """Get all projects on the site"""
        endpoint = f"/sites/{self.site_id}/projects"
        response = self.get(endpoint)
        data = response.json()
        return data["projects"]["project"]
    
    def sign_out(self):
        """Sign out from Tableau Server"""
        try:
            endpoint = "/auth/signout"
            self.post(endpoint)
            logger.info("🚪 Signed out successfully")
        except Exception as e:
            logger.warning(f"Error during sign out: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - sign out"""
        self.sign_out()