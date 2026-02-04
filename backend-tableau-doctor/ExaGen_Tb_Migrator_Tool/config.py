"""
Configuration management for Tableau Migration Tool
"""

import os
from dataclasses import dataclass
from typing import Optional
from pathlib import Path


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: str
    database: str
    username: str
    password: str


@dataclass
class Config:
    """Tableau Server configuration"""
    server_url: str
    api_version: str
    pat_name: str
    pat_secret: str
    site_content_url: str
    database: Optional[DatabaseConfig] = None
    
    @classmethod
    def from_env(cls, environment: str = 'dev') -> 'Config':
        """
        Load configuration from environment variables
        
        Environment variables should be prefixed with the environment:
        - DEV_TABLEAU_SERVER
        - TEST_TABLEAU_SERVER
        - PROD_TABLEAU_SERVER
        etc.
        """
        prefix = environment.upper()
        
        # Tableau configuration
        server_url = os.getenv(f'{prefix}_TABLEAU_SERVER')
        api_version = os.getenv(f'{prefix}_TABLEAU_API_VERSION', '3.23')
        pat_name = os.getenv(f'{prefix}_TABLEAU_PAT_NAME')
        pat_secret = os.getenv(f'{prefix}_TABLEAU_PAT_SECRET')
        site_content_url = os.getenv(f'{prefix}_TABLEAU_SITE_CONTENT_URL', '')
        
        # Validate required fields
        if not all([server_url, pat_name, pat_secret]):
            raise ValueError(
                f"Missing required environment variables for {environment}. "
                f"Required: {prefix}_TABLEAU_SERVER, {prefix}_TABLEAU_PAT_NAME, "
                f"{prefix}_TABLEAU_PAT_SECRET"
            )
        
        # Database configuration (optional)
        db_config = None
        db_host = os.getenv(f'{prefix}_DB_HOST')
        if db_host:
            db_config = DatabaseConfig(
                host=db_host,
                port=os.getenv(f'{prefix}_DB_PORT', '3306'),
                database=os.getenv(f'{prefix}_DB_NAME', ''),
                username=os.getenv(f'{prefix}_DB_USERNAME', ''),
                password=os.getenv(f'{prefix}_DB_PASSWORD', '')
            )
        
        return cls(
            server_url=server_url,
            api_version=api_version,
            pat_name=pat_name,
            pat_secret=pat_secret,
            site_content_url=site_content_url,
            database=db_config
        )
    
    @classmethod
    def from_file(cls, config_path: Path, environment: str = 'dev') -> 'Config':
        """Load configuration from a JSON or YAML file"""
        import json
        
        with open(config_path) as f:
            config_data = json.load(f)
        
        env_config = config_data.get(environment, {})
        
        # Database configuration
        db_config = None
        if 'database' in env_config:
            db_data = env_config['database']
            db_config = DatabaseConfig(**db_data)
        
        return cls(
            server_url=env_config['server_url'],
            api_version=env_config.get('api_version', '3.23'),
            pat_name=env_config['pat_name'],
            pat_secret=env_config['pat_secret'],
            site_content_url=env_config.get('site_content_url', ''),
            database=db_config
        )


# Example configuration file structure (config.json):
"""
{
    "dev": {
        "server_url": "https://us-west-2b.online.tableau.com",
        "api_version": "3.23",
        "pat_name": "Metadata_API_Access",
        "pat_secret": "your-secret-here",
        "site_content_url": "exavalu",
        "database": {
            "host": "mysql-dc333bb-exavalu-2a41.j.aivencloud.com",
            "port": "16528",
            "database": "exavalu",
            "username": "avnadmin",
            "password": "your-password-here"
        }
    },
    "prod": {
        "server_url": "https://us-west-2b.online.tableau.com",
        "api_version": "3.23",
        "pat_name": "Metadata_API_Access",
        "pat_secret": "your-secret-here",
        "site_content_url": "exavalu",
        "database": {
            "host": "mysql-3d9871e1-rahulneogi5043-1a29.j.aivencloud.com",
            "port": "11510",
            "database": "exavalu",
            "username": "avnadmin",
            "password": "your-password-here"
        }
    }
}
"""
