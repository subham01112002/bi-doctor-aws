import requests
import logging
import os

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class PBIXDOwnloader:
    def __init__(self, auth):
        self.auth_manager = auth
        self.base_url = self.auth_manager.config.get_api_base_url()
        self.access_token = self.auth_manager.get_access_token()
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-type': 'application/zip'
        }

    def get_binary_pbix(self, report_id):
        '''
            This method will make an API call to the /Export api with the report id,
            to get a binary file for the .pbix
        '''
        endpoint = f'{self.base_url}/reports/{report_id}/Export'
        print(endpoint)
        try:
            response = requests.get(endpoint, headers=self.headers, timeout=120, stream=True)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4xx and 5xx)
            return response
        except requests.exceptions.HTTPError as e:
            logging.error(f'HTTP Error: {e} - Status Code: {e.response.status_code}')
        except requests.exceptions.ConnectionError as e:
            logging.error(f'Connection Error: {e}')
        except requests.exceptions.Timeout as e:
            logging.error(f'Timeout Error: {e}')
        except requests.exceptions.RequestException as e:
            logging.error(f'Request Exception: {e}')
        except Exception as e:
            logging.critical(f'Critical Error Occurred: {e}')

    def export_pbix(self, response, report_name):
        '''
            Creates a .pbix file with the provided report name

            Parameters:
            response: Response from the API
            report_name: The report name for the .pbix file
        '''
        file_path = 'output/'

        # Ensure that the output directory exists
        os.makedirs(file_path, exist_ok=True)

        try:
            # Open the .pbix file in binary write mode.
            # Write the content from the response stream in chunks of 8192 bytes
            # This is done to efficiently handle large data without consuming too much memory
            with open(f'{file_path}{report_name}.pbix') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

        except FileNotFoundError as e:
            logging.error(f'File Not Found Error: {e}')
            raise
        except IOError as e:
            logging.error(f'IO Error: {e}')
            raise
        except Exception as e:
            logging.critical(f'Critical Error: {e}')
            raise