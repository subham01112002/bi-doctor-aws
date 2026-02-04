"""
Module: tableau_auth_client

This module provides the `TableauAuthClient` class, which handles authentication and session management
for Tableau's REST API. It allows signing in using personal access tokens and signing out from Tableau Server
or Tableau Online.

Dependencies:
- requests: Used to send HTTP requests to the Tableau server.
- json: Handles JSON encoding and decoding.
- logging: Captures any errors or warnings during the API communication.

Class: TableauAuthClient
    - Handles authentication and session management for Tableau's REST API.
    
Methods:
    - __init__: Initializes the TableauAuthClient with configuration details.
    - sign_in: Authenticates the user and retrieves an authentication token and site ID.
    - sign_out: Signs the user out and clears the authentication session.

Key Features:
- Personal Access Token Authentication: Supports authentication via personal access tokens for secure access to Tableau services.
- Session Management: Maintains and clears authentication sessions to ensure proper management of user sessions.
- Error Handling: Includes comprehensive error handling for request failures, JSON parsing issues, and unexpected errors.

Usage Example:
    config = TableauConfigManager()  # Load configuration
    auth_client = TableauAuthClient(config=config)  # Initialize authentication client
    auth_token, site_id = auth_client.sign_in()  # Sign in to Tableau
    auth_client.sign_out()  # Sign out from Tableau

Logging:
- The application uses Python's logging module to log important events, including sign-in and sign-out actions, 
  and any errors encountered during these processes, aiding in monitoring and debugging.
"""

import requests
import json
import logging
from util.config_managers.tableau_reader import TableauConfigManager

# Set up basic logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TableauAuthClient:
    def __init__(self, config: TableauConfigManager, token_name=None, token_value=None,auth_token: str | None = None,
        site_id: str | None = None):
        self.configFile = config

        self.server_url = self.configFile.get_server_url()
        self.api_version = self.configFile.get_api_version()
        self.site_id = self.configFile.get_site_id()
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        # If UI provided token → use it
        # Else → fallback to YAML
        
        logger.info("TableauAuthClient initialized.")  # Log initialization
        #  SESSION MODE
        if auth_token and site_id:
            self.auth_token = auth_token
            self.site_id = site_id
            # self.headers = {
            #     "X-Tableau-Auth": auth_token,
            #     "Content-Type": "application/json",
            # }
            return

        # LOGIN MODE
        if not token_name or not token_value:
            raise Exception("PAT credentials required for login")

        self.pat_token_name = token_name
        self.pat_token = token_value
        # self.headers = {
        #     'Content-Type': 'application/json',
        #     'Accept': 'application/json'
        # }


    def sign_in(self):
        try:
            url = f"{self.server_url}/api/{self.api_version}/auth/signin"  # Sign-in URL
            payload = {
                'credentials': {
                    'personalAccessTokenName': self.pat_token_name,
                    'personalAccessTokenSecret': self.pat_token,
                    'site': {
                        'contentUrl': self.site_id
                    }
                }
            }

            response = requests.post(url, json=payload, headers=self.headers)  # Sign-in request
            logger.info("Sign-in request sent.")  # Log request sent

            if response.status_code == 200:
                self.auth_token = response.json()['credentials']['token']  # Extract auth token
                self.site_id = response.json()['credentials']['site']['id']  # Extract site ID
                self.user_id = response.json()['credentials']['user']['id']
                logger.info(f"Signed in successfully as user ID: {self.user_id}")  # Log successful sign-in with user ID
                # Fetch username using a separate API call
                self.username = self.get_current_user()
                logger.info(f"Signed in successfully as user: {self.username}")
                return self.auth_token, self.site_id, self.username  # Return authentication details
            else:
                logger.error("Failed to sign in with status code: %s", response.status_code)  # Log failure
                response.raise_for_status()  # Raise error for unsuccessful requests

        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request: {e}")  # Log request errors
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response: {e}")  # Log JSON parsing errors
            raise
        except KeyError as e:
            logger.error(f"Error accessing JSON response: {e}")  # Log key errors in the response
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")  # Log any unexpected errors
            raise

    def jwt_sign_in(self, encoded_token):
        self.jwt_token = encoded_token
        try:
            url = f"{self.server_url}/api/{self.api_version}/auth/signin"  # Sign-in URL
            payload = {
                'credentials': {
                    "jwt": self.jwt_token,
                    'site': {
                        'contentUrl': self.site_id
                    }
                }
            }
            logger.info(f"Attempting JWT sign-in... site: {self.site_id}")  # Log sign-in attempt

            response = requests.post(url, json=payload, headers=self.headers)  # Sign-in request
            logger.info("Sign-in request sent.")  # Log request sent

            if response.status_code == 200:
                self.auth_jwt_token = response.json()['credentials']['token']  # Extract auth token
                self.site_id = response.json()['credentials']['site']['id']  # Extract site ID
                # self.user_id = response.json()['credentials']['user']['id']  # Extract user ID
                logger.info("Signed in successfully.")  # Log successful sign-in
                return self.auth_jwt_token, self.site_id  # Return authentication details
            else:
                logger.error("Failed to sign in with status code: %s", response.status_code)  # Log failure
                response.raise_for_status()  # Raise error for unsuccessful requests

        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request: {e}")  # Log request errors
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response: {e}")  # Log JSON parsing errors
            raise
        except KeyError as e:
            logger.error(f"Error accessing JSON response: {e}")  # Log key errors in the response
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")  # Log any unexpected errors
            raise

    def sign_out(self):
        try:
            if self.auth_token is None:
                logger.warning("You are not signed in.")  # Warn if not authenticated
                return

            url = f"{self.server_url}/api/{self.api_version}/auth/signout"  # Sign-out URL
            self.headers['X-Tableau-Auth'] = self.auth_token  # Add auth token to headers
            response = requests.post(url, headers=self.headers)  # Sign-out request
            logger.info("Sign-out request sent.")  # Log request sent
            response.raise_for_status()  # Raise error for failed requests

            self.auth_token = None  # Clear auth token
            self.site_id = None    # Clear site ID
            logger.info("Signed out successfully.")  # Log successful sign-out

        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request: {e}")  # Log request errors
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")  # Log any unexpected errors
            
            
    def get_current_user(self):
        """
        Fetch the current user's details using the user ID from sign-in.
        """
        try:
            url = f"{self.server_url}/api/{self.api_version}/sites/{self.site_id}/users/{self.user_id}"
            headers = self.headers.copy()
            headers['X-Tableau-Auth'] = self.auth_token
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                user_data = response.json()
                username = user_data['user']['name']
                logger.info(f"Retrieved username: {username}")
                return username
            else:
                logger.warning(f"Failed to retrieve username. Status code: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error retrieving username: {e}")
            return None        