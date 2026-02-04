"""
Module: tableau_config_manager

This module defines the `TableauConfigManager` class, which is responsible for
managing and accessing Tableau configuration settings. The class follows the
Singleton design pattern to ensure a single instance is used throughout the
application, providing consistent access to configuration values.

Key Responsibilities:
- Reading and loading configuration settings from a YAML file.
- Providing access to specific configuration values such as server URL,
  API version, authentication token, and site ID.
- Handling errors related to file access and configuration parsing.

Usage:
- Instantiate the class to access configuration values.
- Methods include:
  - `get_server_url()`: Returns the Tableau server URL.
  - `get_api_version()`: Returns the Tableau API version.
  - `get_auth_token()`: Returns the authentication token value.
  - `get_auth_token_name()`: Returns the authentication token name.
  - `get_site_id()`: Returns the Tableau site ID.

Error Handling:
- Handles `FileNotFoundError` and `yaml.YAMLError` during configuration file
  reading and parsing.
- Provides error messages if required configuration keys are missing.

Example:
    config = TableauConfigManager()
    server_url = config.get_server_url()
    api_version = config.get_api_version()
"""

import yaml
import os
import logging

# Set up basic logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Singleton class to read the Tableau config
class TableauConfigManager:
    _instance = None  # Class-level instance to ensure only one instance exists (Singleton pattern)

    # Overriding the __new__ method to implement Singleton pattern
    def __new__(cls):
        if cls._instance is None:
            # If instance does not exist, create one and load the config
            cls._instance = super(TableauConfigManager, cls).__new__(cls)
            cls._instance.config = cls._read_config()
        return cls._instance

    # Static method to read the configuration file
    @staticmethod
    def _read_config():
        config_file_path = os.path.join('./config', 'tableau.yaml')  # Path to the config file
        try:
            with open(config_file_path, 'r') as file:
                # Load the YAML file into a Python dictionary
                logger.info("Reading configuration file at %s", config_file_path)
                return yaml.safe_load(file)
        except FileNotFoundError:
            logger.error("Config file not found at %s", config_file_path)
            raise
        except yaml.YAMLError as e:
            logger.error("Failed to parse config file: %s", e)
            raise

    # Getter for Tableau server URL
    def get_server_url(self):
        try:
            url = self.config['tableau']['server']['url']
            logger.info("Retrieved server URL: %s", url)
            return url
        except KeyError:
            logger.error("Server URL not found in config file")
            raise

    # Getter for Tableau API version
    def get_api_version(self):
        try:
            version = self.config['tableau']['api']['version']
            logger.info("Retrieved API version: %s", version)
            return version
        except KeyError:
            logger.error("API version not found in config file")
            raise

    # Getter for Tableau auth token value
    def get_pat_token(self):
        try:
            token = self.config['tableau']['auth']['token']['value']
            logger.info("Retrieved auth token")
            return token
        except KeyError:
            logger.error("Auth token not found in config file")
            raise
        
    # Getter for Tableau auth token name
    def get_pat_token_name(self):
        try:
            token_name = self.config['tableau']['auth']['token']['name']
            logger.info("Retrieved auth token name: %s", token_name)
            return token_name
        except KeyError:
            logger.error("Auth token name not found in config file")
            raise

    # Getter for Tableau site ID
    def get_site_id(self):
        try:
            site_id = self.config['tableau']['site']['id']
            logger.info("Retrieved site ID: %s", site_id)
            return site_id
        except KeyError:
            logger.error("Site ID not found in config file")
            raise

    # Getter for Tableau output path directory
    def get_output_directory(self):
        try:
            output_path = self.config['tableau']['output']['directory']
            logger.info("Retrieved output path: %s", output_path)
            return output_path
        except KeyError:
            logger.error("Output path not found in config file")
            raise
        
    # Getter for Tableau output path directory
    def get_logo_path(self):
        try:
            logo_path = self.config['tableau']['image']['logopath']
            logger.info("Retrieved logo path: %s", logo_path)
            return logo_path
        except KeyError:
            logger.error("Logo path not found in config file")
            raise