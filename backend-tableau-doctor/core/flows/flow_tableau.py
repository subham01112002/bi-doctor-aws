import urllib.parse
import logging
import os
import yaml
from core.ui.main_window import MainWindow
from PyQt5.QtCore import pyqtSignal, QObject
from util.config_managers.tableau_reader import TableauConfigManager
from util.auth_clients.tableau_auth import TableauAuthClient
from util.query_clients.tableau_query_client import TableauQueryClient
from core.models.tableau_dropdown_loader_models import DropdownLoaderResponse
from core.models.tableau_workbook_models import WorkbooksResponse
from util.tableau_excel_generator import TableauExcellGenerator
from core.managers.tableau_data_manager import TableauDataManager

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class TableauFlow(QObject):
    # Signal variables are defined here
    tool_change = pyqtSignal(str)   
    projects_change = pyqtSignal(str)
    
    def __init__(self, window:MainWindow, token_name=None, token_value=None):

        logging.info('Starting: Tableau Flow')

        super().__init__()

        self.window = window
        # Store token from login screen
        self.token_name = token_name
        self.token_value = token_value
        self.connect_signals()  # Connection the signals
        self.define_widgets()   # Define the wigdets requried for this class             
        self.load_client()
        
    def load_client(self):     
        logging.info('Loading: Tableau Clinet')   
        # Read the tableau config file
        self.config = TableauConfigManager()
        # Initiatie the tableau auth client
        self.auth = TableauAuthClient(
                        config=self.config,
                        token_name=self.token_name,
                        token_value=self.token_value
                    )       
        # Sign-in
        self.auth.sign_in()                
        # Create the query client
        self.client = TableauQueryClient(auth=self.auth)
    
    def define_widgets(self):
        # Widgets
        self.wgt_projects = self.window.central_container.middle_left.wgt_project_selector
        self.wgt_reports = self.window.central_container.middle_left.wgt_report_selector
        # Buttons
        self.btn_download = self.window.central_container.middle_left.btn_dwnload
        
        self.btn_clear_filter = self.window.central_container.middle_left.wgt_bi_selector.btn_clear_filter
        
    def connect_signals(self):
        self.tool_change.connect(self.load_projects)
        #self.projects_change.connect(self.on_projects_change)
        self.window.central_container.middle_left.wgt_bi_selector.btn_clear_filter.clicked.connect(self.reset_filters)
 
        
    def load_projects(self):
        '''
            This method will load the Projects dropdown, on the BI Tool dropdown change event.
        '''
        try:            
            query = self.client.query_loader()
            logging.debug(f'Query Loaded: {query}')
            
            response = self.client.send_request(query=query)    
            data = DropdownLoaderResponse(**response['data'])
            
            projects_list = []
            seen_projects = set()
            
            # Iterate through each workbook in the loaded data
            for workbook in data.workbooks:
                project = {
                    'id': workbook.projectVizportalUrlId,
                    'name': workbook.projectName
                }
                
                # Checking for already added projects
                if project['id'] not in seen_projects:
                    projects_list.append(project)
                    seen_projects.add(project['id'])                    
                    
            logging.info(f'Retrieved {len(projects_list)} unique projects from Tableau.')
            
            # Reset the projects combobox
            self.wgt_projects.reset_cbx()
            logging.info('Resetting: Projects Dropdown')
            # Load the projects combobox
            self.wgt_projects.update_projects(project_list=projects_list)
            # Connecting the projects dropdown
            self.wgt_projects.cbx_project.currentTextChanged.connect(self.load_reports)           
            logging.info('Connecting: Projects Dropdown')
        except AttributeError as e:
            logging.error(f'Attribute Error: {e}')
            self.window.mbox.critical(self.window,'Error: ', f'Attribute Error: {e}')
        except TypeError as e:
            logging.error(f'Type Error: {e}')
            self.window.mbox.critical(self.window, 'Error', f'Type Error: {e}')
        except IndexError as e:
            logging.error(f'Index Error: {e}')
            self.window.mbox.critical(self.window, 'Error', f'Index Error: {e}')
        except RuntimeError as e:
            logging.error(f'Runtime Error: {e}')
            self.window.mbox.critical(self.window, 'Error', f'Runtime Error: {e}')
        except ConnectionError as e:
            logging.error(f'Connection Error: {e}')
            self.window.mbox.critical(self.window,'Error', f'Connection Error: {e}')
        except Exception as e:
            logging.critical(f'Critical Error: {e}')
            self.window.mbox.critical(self.window, 'Critical', f'Critical Exception: {e}')
    
    def load_reports(self, current_project):
        '''
            This method will load the Reports reports multiselct in the UI, on the projects dropdown
            change event
        '''        
        try:
            selected_project_id = self.wgt_projects.cbx_project.currentData()
            
            if current_project == 'Select:':
                
                # Clear Widgets if the selected Project is 'Select'
                self.wgt_reports.clear()
                
                # Disconnect the download button signal
                try:
                    self.btn_download.disconnect()
                    logging.info('Disconnected: Get Data button signal.')
                except TypeError as e:
                    logging.warning(f'No Actice Connection: Get Data Button: {e}')

            else:
                
                query = self.client.query_loader() 
                logging.debug(f'Query loaded: {query}')
                
                response = self.client.send_request(query=query) 
                
                data = DropdownLoaderResponse(**response['data'])
                logging.info('Workbook data loaded successfully.')
                
                report_list = []
                seen_reports = set()
                seen_reports_luid = set()
                # Iterate through each workbook in the laoded data
                for workbook in data.workbooks:
                    # Check if the workbook belongs to the specified project
                    if workbook.projectVizportalUrlId == selected_project_id:
                        report = {
                            'reportId': workbook.id,
                            'reportLuid': workbook.luid,
                            'reportName': workbook.name,
                        }
                        
                        # Ensure that the report id is unique
                        if report['reportId'] not in seen_reports:
                            report_list.append(report)
                            seen_reports.add(report['reportId'])
                            seen_reports_luid.add(report['reportLuid'])
                            
                self.wgt_reports.reset_cbx() # Reset the reports dropdown
                self.wgt_reports.add_reports(report_list=report_list) #Add the reports in the UI

                    # Connect the download button
                try:
                    self.btn_download.clicked.disconnect()
                    logging.debug('Previous download connection removed.')
                except TypeError:
                    logging.debug('No previous download connection found.')
                    
                    # Connecting Download Button
                self.btn_download.clicked.connect(self.on_download)
                                        
        except AttributeError as e:
            logging.error(f'Attribute Error: {e}')
            self.window.mbox.critical(self.window,'Error: ', f'Attribute Error: {e}')
        except TypeError as e:
            logging.error(f'Type Error: {e}')
            self.window.mbox.critical(self.window, 'Error', f'Type Error: {e}')
        except IndexError as e:
            logging.error(f'Index Error: {e}')
            self.window.mbox.critical(self.window, 'Error', f'Index Error: {e}')
        except RuntimeError as e:
            logging.error(f'Runtime Error: {e}')
            self.window.mbox.critical(self.window, 'Error', f'Runtime Error: {e}')
        except ConnectionError as e:
            logging.error(f'Connection Error: {e}')
            self.window.mbox.critical(self.window,'Error', f'Connection Error: {e}')
        except Exception as e:
            logging.critical(f'Critical Error: {e}')
            self.window.mbox.critical(self.window, 'Critical', f'Critical Exception: {e}')
    
    def on_download(self):
        '''
            This method will handle the Download button click event.
        '''
        
        original_btn_text = self.btn_download.text()
        original_stylesheet = self.btn_download.styleSheet()
        try:
            
            # Disable the button
            self.btn_download.setEnabled(False) 
            
            # Apply the green loading style
            green_style = """
            QPushButton {
                background-color: #4CAF50; /* Green color */
                color: white; /* White text for contrast */
                border: 1px solid #4CAF50; 
                padding: 5px;
            }
            """
            self.btn_download.setStyleSheet(green_style)
            
            # Show the text "Processing..."
            self.btn_download.setText("Processing...") 
            self.window.repaint() # Important for immediate visual update
            
            # Check for no reports selected
            if not self.wgt_reports.checked_report_ids:
                logging.warning('No Reports Selected.')
                raise ValueError('No reports selected! Please select a report.')
            
            # Retrieve the selected report IDs
            select_report_ids = self.wgt_reports.get_selected_reports()
            select_report_luids = self.wgt_reports.get_selected_report_luids()
            # Retrieve the full workbook data
            query = self.client.query_workbook_metadata(workbook_ids=select_report_ids) # Get the Query
            response = self.client.send_request(query=query) # Get Response in json format
            usage_stats_response = self.client.get_usage_stats_wb(workbook_luids=select_report_luids)  # Get Usage Stats Response in flatten format
            # Parsing response to Data Model
            self.full_workbook_data = WorkbooksResponse(**response['data'])

            self.data_manager = TableauDataManager(full_workbook_data=self.full_workbook_data)  

            flat_data_wb = self.data_manager.get_flat_wb_data()
            flat_data_embd, flat_data_query = self.data_manager.get_flat_embd_data()

            package = []
            
            # Prepare the package for the Dashboard details sheet
            package.append({
                'sheet_name':'Dashboard Details',
                'payload':flat_data_wb,
                'columns':[
                    'Project',
                    'Workbook',
                    'Dashboard',
                    'Sheet',
                    'Field',
                    'Field Type',
                    'Datasource',
                    'Table Name',
                    'Column Name',
                    'Formula'
                ]
            })
            
            # Prepare the package for the Datasource Details sheet
            package.append({
                'sheet_name':'Datasource Details',
                'payload':flat_data_embd,
                'columns':[
                    'Project',
                    'Workbook',
                    'Datasource',
                    'Table',
                    'Column',
                    'Used In Sheet',
                    'Custom Query'
                ]
            })
            
            # Prepare the package for the Custom Query Details sheet
            package.append({
                'sheet_name':'Custom Query Details',
                'payload':flat_data_query,
                'columns':[
                    'Project',
                    'Workbook',
                    'Custom Query',
                    'Query'
                ]
            })    

            package.append({
                'sheet_name':'Usage Statistics',
                'payload':usage_stats_response,
                'columns':[
                    'project',
                    'Workbook',
                    'Data Source',
                    'View',
                    'created_at',
                    'updated_at',
                    'Total Views'
                ]
            })   
            
            logging.info('Generate Spreadsheet Flow: Start')
            self.generate_spreadsheet(package=package)    
                 
        except AttributeError as e:
            logging.error(f'Attribute Error: {e}')
            self.window.mbox.critical(self.window,'Error: ', f'Attribute Error: {e}')
        except TypeError as e:
            logging.error(f'Type Error: {e}')
            self.window.mbox.critical(self.window, 'Error', f'Type Error: {e}')
        except IndexError as e:
            logging.error(f'Index Error: {e}')
            self.window.mbox.critical(self.window, 'Error', f'Index Error: {e}')
        except RuntimeError as e:
            logging.error(f'Runtime Error: {e}')
            self.window.mbox.critical(self.window, 'Error', f'Runtime Error: {e}')
        except ConnectionError as e:
            logging.error(f'Connection Error: {e}')
            self.window.mbox.critical(self.window,'Error', f'Connection Error: {e}')
        except Exception as e:
            logging.critical(f'Critical Error: {e}')
            self.window.mbox.critical(self.window, 'Critical', f'Critical Exception: {e}')
        finally:
            # Restore the original text
            self.btn_download.setText(original_btn_text)
            
            # Restore the original stylesheet/appearance
            self.btn_download.setStyleSheet(original_stylesheet)
            
            # Re-enable the button
            self.btn_download.setEnabled(True)
 
        
    def generate_spreadsheet(self, package):
        '''
            On Downlaod button click this method will be called after the package has been created.
            This method will then generate the Primary Sheets and then the summary sheet.
            This method will then format all the sheets.
        '''    
        try:
            excell_genrator = TableauExcellGenerator(package=package)
            excell_genrator.generate_spreadsheet()
            excell_genrator.format_excel()        
            self.write_summary_counts(excell_generator=excell_genrator)
            with open(os.path.join("config", "tableau.yaml"), "r") as f:
                cfg = yaml.safe_load(f)
            output_dir = cfg["tableau"]["output"]["directory"]
 
            # Include output directory in the message
            #msg = f"Successfully generated the metadata spreadsheet at:\n{output_dir}"
            abs_path = os.path.abspath(output_dir)
            # 2. Convert C:\ to C| (standard URI convention for Windows drives)
            uri_path = abs_path.replace('\\', '/')
            quoted_path = urllib.parse.quote(uri_path)
            # 3. URL-encode and prepend file:///
            hyperlink_url = f"file:///{quoted_path}"

            # The text the user sees
            display_text = output_dir

            # The HTML string
            html_link = f'<a href="{hyperlink_url}">{display_text}</a>'

            # The final message ready for PyQt
            msg = f"Successfully generated the metadata spreadsheet at:<br>{html_link}"
            self.window.mbox.information(self.window, 'Excel Generated', msg)
        except PermissionError as e:
            logging.error(f'Permission Error: {e}')
            self.window.mbox.critical(self.window, 'Error', f'Permission Error: {e}')
        except Exception as e:
            logging.critical(f'Critical Error: {e}')
            self.window.mbox.critical(self.window, 'Critical Error', f'Critical Error: {e}')
        finally:
            logging.info('Generate Spreadsheet Flow: End')
        
        
    # def write_workbook_counts(self, excell_generator):
    #     try:
    #         unique_counts_workbook = self.data_manager.get_workbook_counts()     
    #         excell_generator.generate_summary_sheet(unique_counts=unique_counts_workbook)
    #         logging.info('Written: Workbook Counts')
    #     except AttributeError as e:
    #         logging.error(f'Attribute Error: {e}')
    #         self.window.mbox.critical(self.window, 'Error', f'Attribute Error: {e}')
    #     except TypeError as e:
    #         logging.error(f'Type Error: {e}')
    #         self.window.mbox.critical(self.window, 'Error', f'Type Error: {e}')
    #     except ValueError as e:
    #         logging.error(f'Value Error: {e}')
    #         self.window.mbox.critical(self.window, 'Error', f'Value Error: {e}')
    #     except Exception as e:
    #         logging.critical(f'Critical Error: {e}')
    #         self.window.mbox.critical(self.window, 'Critical Error', f'Critical Runtime Error: {e}')
    
    # def write_datasource_counts(self, excell_generator):
    #     try:
    #         unique_counts_datasource = self.data_manager.get_datasource_counts()
    #         excell_generator.generate_summary_sheet(unique_counts=unique_counts_datasource)       
    #         logging.info('Written: Datasource and Query Counts')
    #     except AttributeError as e:
    #         logging.error(f'Attribute Error: {e}')
    #         self.window.mbox.critical(self.window, 'Error', f'Attribute Error: {e}')
    #     except TypeError as e:
    #         logging.error(f'Type Error: {e}')
    #         self.window.mbox.critical(self.window, 'Error', f'Type Error: {e}')
    #     except ValueError as e:
    #         logging.error(f'Value Error: {e}')
    #         self.window.mbox.critical(self.window, 'Error', f'Value Error: {e}')
    #     except Exception as e:
    #         logging.critical(f'Critical Error: {e}')
    #         self.window.mbox.critical(self.window, 'Critical Error', f'Critical Runtime Error: {e}')
        
        
    def write_summary_counts(self, excell_generator):
        """
        Generate a combined summary sheet showing workbook-level counts
        for dashboards, sheets, fields, datasources, tables, and columns.
        """
        try:
            # Retrieve both sets of data
            workbook_counts = self.data_manager.get_workbook_counts()
            datasource_counts = self.data_manager.get_datasource_counts()
 
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
 
            # Prepare the package for Excel
            package = [{
                "sheet_name": "Workbook Summary",
                "payload": summary_data,
                "columns": summary_columns
            }]
 
            # Write the combined summary sheet
            excell_generator.generate_summary_sheet(unique_counts=summary_data, columns=summary_columns)
            logging.info("Workbook Summary sheet written successfully.")
 
        except Exception as e:
            logging.critical(f"Critical Error generating summary sheet: {e}")
            self.window.mbox.critical(self.window, "Critical Error", f"Error while writing summary:\n{e}")
     
        
    def reset_filters(self):
        """
        Clears all Tableau selections, disconnects signals,
        and resets UI to its initial state.
        """
        try:
            logging.info("Resetting Tableau selections...")
 
            # Disconnect signals to prevent unwanted triggers
            try:
                if self.wgt_projects.cbx_project.receivers(self.wgt_projects.cbx_project.currentTextChanged):
                    self.wgt_projects.cbx_project.currentTextChanged.disconnect(self.load_reports)
                    logging.debug("Disconnected project change signal.")
            except TypeError:
                logging.debug("No active project change connection to disconnect.")
 
            try:
                if self.btn_download.receivers(self.btn_download.clicked):
                    self.btn_download.clicked.disconnect(self.on_download)
                    logging.debug("Disconnected download button signal.")
            except TypeError:
                logging.debug("No active download signal to disconnect.")
 
            # Reset widgets
            self.wgt_projects.cbx_project.clear()
            self.wgt_reports.clear()
 
            # Optionally reset BI tool dropdown if present
            if hasattr(self.window.central_container.middle_left.wgt_bi_selector, "cbx_biTool"):
                cbx = self.window.central_container.middle_left.wgt_bi_selector.cbx_biTool
                cbx.blockSignals(True)
                cbx.setCurrentIndex(0)
                cbx.blockSignals(False)
                logging.debug("BI tool dropdown reset.")
                logging.info("Reloading Tableau projects after reset...")
                self.load_projects()
 
            logging.info("All Tableau filters cleared successfully.")
 
        except Exception as e:
            logging.error(f"Error resetting Tableau filters: {e}")
            self.window.mbox.critical(self.window, "Reset Error", f"Error resetting filters:\n{e}")
 
 