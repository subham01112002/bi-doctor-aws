"""
TableauQueryClient Module

This module defines the `TableauQueryClient` class, which is responsible for 
interacting with the Tableau GraphQL API to execute queries related to workbooks 
and their metadata. It utilizes the `TableauAuthClient` for authentication 
and provides methods to send queries and process responses.

Imports:
- requests: Used for making HTTP requests to the Tableau API.
- json: Used for handling JSON data.
- logging: Used for logging information and errors.
- util.auth_clients.tableau_auth: Provides the TableauAuthClient for authentication.

Classes:
- TableauQueryClient: Client for querying Tableau metadata and workbooks via GraphQL.
"""

import requests  # Import the requests library for HTTP requests
import json  # Import the json library for JSON handling
import logging  # Import the logging library for logging
from util.auth_clients.tableau_auth import TableauAuthClient  # Import TableauAuthClient

# Set up logging configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class TableauQueryClient:
    """
    A client for querying Tableau workbooks and metadata using the GraphQL API.
    """

    def __init__(self, auth: TableauAuthClient):
        """Initialize the TableauQueryClient with the given authentication client."""
        self.auth_client = auth  # Store the TableauAuthClient instance
        self.headers = {
            'Content-Type': 'application/json',  # Set content type for requests
            'Accept': 'application/json'  # Set accepted response type
        }  # Headers used for GraphQL requests

    def query_loader(self):
        """Construct and return a query to load workbooks."""
        try:
            query = """{
                workbooks {
                    name
                    id
                    luid
                    projectName
                    projectLuid
                    projectVizportalUrlId
                    upstreamDatasources{
                     luid
                     name
                    }
                    embeddedDatasources {
                    id
                    name
                    parentPublishedDatasources{
                        name
                        luid
                    }
                  }
                }
            }"""
            return query  # Return the constructed query
        except Exception as e:
            logging.critical(f'Runtime Critical Error: {e}')
            raise  # Raise any exceptions without handling

    def query_workbook_metadata(self, workbook_ids):
        """Construct a query for retrieving metadata for the specified workbooks."""
        workbook_id_json = json.dumps(workbook_ids)  # Convert workbook names to JSON format
        try:
            query = f"""
            {{
                workbooks(filter: {{ idWithin: {workbook_id_json} }}) {{
                    name
                    id
                    createdAt
  					updatedAt
  					luid
  					tags{{
                      name
                    }}
  					description
                    projectName
                    projectVizportalUrlId
                    owner {{
                        id
                        username
                    }}
                    dashboards {{
                        name
                        id
                        sheets {{
                            name
                            id
                            datasourceFields {{
                                id
                                name
                                datasource {{
                                    id
                                    name
                                }}
                                upstreamColumns {{
                                    name
                                    table {{ name }}
                                }}
                                __typename
                                ... on CalculatedField {{
                                    formula
                                }}
                            }}
                        }}
                    }}
                    embeddedDatasources {{
                        id
                        name
                        createdAt
                        updatedAt
                        hasExtracts
                        fields{{
                            id
                            name
                            __typename
                            ... on CalculatedField {{
                                formula
                            }}
                        }}
                        upstreamTables {{
                            name
                            referencedByQueries {{
                                id
                                name
                                query
                                columns {{
                                    name
                                    downstreamWorkbooks {{
                                        id
                                    }}
                                }}
                            }}
                            columns {{
                                name
                                downstreamWorkbooks {{
                                    id
                                }}
                            }}
                        }}  
                    }}                  
                }}
            }}
            """  # Construct and return the metadata query
            
            return query
        except ValueError as e:
            logging.error(f'Value Error: {e}')
            raise
        except Exception as e:
            logging.critical(f'Runtime Critical Error: {e}')
            raise

    def query_datasource(self):
        """Construct and return a query to load datasources."""
        try:
            query = """{
                publishedDatasources{
                        id
                        name
                        luid
                        projectVizportalUrlId
                        projectName
                    }
            }"""
            return query  # Return the constructed query
        except Exception as e:
            logging.critical(f'Runtime Critical Error: {e}')
            raise  # Raise any exceptions without handling

    def query_datasource_metadata(self, datasource_ids):
        """Construct a query for retrieving metadata for the specified datasources."""
        datasource_id_json = json.dumps(datasource_ids)  # Convert datasource IDs to JSON format
        try:
            query = f"""{{
                    publishedDatasources(filter: {{ idWithin: {datasource_id_json} }}) {{
                        id
                        luid
                        name
                        createdAt
                        updatedAt
                        hasExtracts
                        __typename
                        projectName
                        projectVizportalUrlId
                        tags {{
                                name
                            }}
                        fields {{
                            id
                            name
                            __typename
                            ... on CalculatedField {{
                                formula
                            }}
                            upstreamColumns {{
                                name
                                table {{
                                    name
                                    downstreamSheets {{
                                        id
                                        name
                                        containedInDashboards {{
                                            id
                                            name
                                        }}
                                        workbook {{
                                            name
                                            id
                                            luid
                                            projectName
                                            updatedAt
                                            description
                                            createdAt
                                            projectVizportalUrlId
                                            owner {{
                                                id
                                                username
                                            }}
                                            tags {{
                                                name
                                            }}
                                        }}
                                    }}
                                }}
                            }}    
                                
                            
                        }}
                        upstreamTables {{
                            name
                            referencedByQueries {{
                                id
                                name
                                query
                                columns {{
                                    name
                                    downstreamWorkbooks {{
                                        id
                                        name
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
                
                    """ # Construct and return the metadata query            
            return query
        except ValueError as e:
            logging.error(f'Value Error: {e}')
            raise
        except Exception as e:
            logging.critical(f'Runtime Critical Error: {e}')
            raise    


    def send_request(self, query):
        """Send a request to the Tableau API with the specified GraphQL query."""
        #logging.info(f"Sending request with query: {query}")  # Log the query being sent
        try:
            if self.auth_client.auth_token is None:
                logging.warning('You are not signed in.')  # Log warning if user is not authenticated
                raise RuntimeError('User is not authenticated.')  # Raise error if not signed in

            url = f'{self.auth_client.server_url}/api/metadata/graphql'  # Construct the API URL
            self.headers['X-Tableau-Auth'] = self.auth_client.auth_token  # Add auth token to headers
            self.headers['Content-Type'] = 'application/json'
            payload = {'query': query}  # Create the payload with the query string

            #logging.info(f"Making POST request to URL: {url} with payload: {payload}")  # Log request details
            response = requests.post(url, json=payload, headers=self.headers)  # Send the POST request
            response.raise_for_status()  # Raise error for unsuccessful status codes
            logging.info("Request successful, processing response.")  # Log success
            #logging.info(f"Response data: {response.json()}")  # Add this line
            return response.json()  # Return JSON response data

        except requests.exceptions.RequestException as e:
            logging.error(f'Request Error: {e}')  # Log HTTP request errors
            raise  # Raise the exception without handling
        except json.JSONDecodeError as e:
            logging.error(f'JSON Decode Error: {e}')  # Log JSON decoding errors
            raise  # Raise the exception without handling
        except KeyError as e:
            logging.error(f'Key Error: {e}')  # Log errors for missing keys in response
            raise  # Raise the exception without handling
        except Exception as e:
            logging.error(f'Runtime Critical Error: {e}')  # Log any other unexpected errors
            raise  # Raise the exception without handling

    def get_usage_stats_wb(self, workbook_luids):

        try:
            if self.auth_client.auth_token is None:
                logging.warning("You are not signed in.")
                raise RuntimeError("User is not authenticated.")
    
            self.headers["X-Tableau-Auth"] = self.auth_client.auth_token
            self.headers["Content-Type"] = "application/json"
            base = f"{self.auth_client.server_url}/api/{self.auth_client.api_version}/sites/{self.auth_client.site_id}"

            all_rows = []
            for wb_luid in workbook_luids:
                try:
                    # 1) Workbook metadata (REST)
                    query = f"""
                    {{
                        workbooks(filter: {{ luid: "{wb_luid}" }}) {{
                            id
                            name
                            projectName
                            projectVizportalUrlId
                    }}
                    }}
                    """
                    workbook_response = self.send_request(query)

                    workbooks = workbook_response.get("data", {}).get("workbooks", [])

                    if not workbooks:
                        continue

                    workbook_id = workbooks[0]["id"]
                    workbook_name = workbooks[0]["name"]
                    project_name = workbooks[0]["projectName"]
                    project_id = workbooks[0]["projectVizportalUrlId"]


                    # 2) Datasources from Metadata API (GraphQL) instead of /connections
                    #datasource_names = self._get_datasources_from_metadata(wb_luid)
        
                    # 3) Views + usage (REST)
                    usage_url = f"{base}/workbooks/{wb_luid}/views?includeUsageStatistics=true"
                    views_res = requests.get(usage_url, headers=self.headers)
                    if views_res.status_code == 404:
                        logging.warning(
                            "Views for workbook %s not found – skipping usage.",
                            wb_luid,
                        )
                        continue
                    views_res.raise_for_status()
                    views = views_res.json()["views"]["view"]
        
                    # 4) Build rows
                    for v in views:
                        usage = v.get("usage", {})
                        row = {
                            "project_id": project_id,
                            "project_name": project_name,
                            "workbook_id": workbook_id,
                            "workbook_name": workbook_name,
                            #"datasources": datasource_names,
                            "view_id": v["id"],
                            "view_name": v["name"],
                            "created_at": v["createdAt"],
                            "updated_at": v["updatedAt"],
                            "total_view_count": int(usage.get("totalViewCount", 0)),
                        }
                        all_rows.append(row)

                except requests.exceptions.RequestException as e:
                    logging.error(f"Request Error for workbook {wb_luid}: {e}")
                    continue  # skip this workbook on error
        
            logging.info("Request successful, processing response.")

            return all_rows
    
        except requests.exceptions.RequestException as e:
            logging.error(f"Request Error: {e}")
            raise