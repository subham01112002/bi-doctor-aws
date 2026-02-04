from core.ui.main_window import MainWindow
from core.flows.flow_tableau import TableauFlow
from PyQt5.QtCore import QSignalBlocker

import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class FlowManager:
    def __init__(self, window:MainWindow,token_name=None, token_value=None):
        self.window = window

        # Store token user typed in login screen
        self.token_name = token_name
        self.token_value = token_value

        self.flow = None       
        self.wgt_bi_tool = self.window.central_container.middle_left.wgt_bi_selector
        self.wgt_projects = self.window.central_container.middle_left.wgt_project_selector
        self.wgt_reports = self.window.central_container.middle_left.wgt_report_selector
        self.btn_download = self.window.central_container.middle_left.btn_dwnload

        self.initial_connections()
            
    def initial_connections(self):
        '''This method initializes all the required singals for when the application loads'''
        self.wgt_bi_tool.cbx_biTool.currentTextChanged.connect(self.on_tool_change)
        # Block signal while setting default
        with QSignalBlocker(self.wgt_bi_tool.cbx_biTool):
            self.wgt_bi_tool.cbx_biTool.setCurrentText("Tableau")
        
        # Manually trigger once
        self.on_tool_change()
            
    def on_tool_change(self):
        self.selected_tool = self.wgt_bi_tool.cbx_biTool.currentText()   
             
        try:
            # Common disconnection logic before any switch
            # try:
            #     self.btn_get_data.disconnect()
            #     logging.info("Disconnected: Get Data button signal (before switching tool).")
            # except TypeError:
            #     pass
            
            try:
                self.btn_download.disconnect()
                logging.info("Disconnected: Download button signal (before switching tool).")
            except TypeError:
                pass
            
            try:
                self.wgt_reports.disconnect()
                logging.info("Disconnected: Report selector signals (before switching tool).")
            except TypeError:
                pass
            
            if self.selected_tool == 'Select Tools':
                self.flow=None
                # Clear Widgets
                self.wgt_projects.cbx_project.clear()
                self.wgt_reports.clear()

                # Make the checkbox for selecting .pbix files from local machine not visible
                self.window.central_container.middle_left.chkbox_select_local_files.setVisible(False)
                logging.info('Hidden: File Selector Checkbox')              
                # Make the checkbox for selecting .pbix disabled for toggling
                self.window.central_container.middle_left.chkbox_select_local_files.setEnabled(False)
                logging.info('Disabled: File Selector Checkbox')

                # Make the file browser button not visible
                self.window.central_container.middle_left.btn_choose_directory.setVisible(False)
                logging.info('Hidden: Choose Directory Button')
                # Make the file browser button disabled
                self.window.central_container.middle_left.btn_choose_directory.setEnabled(False)
                logging.info('Disabled: Choose Directory Button')

                
                
                    
            elif self.selected_tool == 'Tableau':
                # Run: Tableau Flow
                self.flow = TableauFlow(
                            window=self.window,
                            token_name=self.token_name,
                            token_value=self.token_value
                        )
                self.flow.tool_change.emit(self.selected_tool)

                # Make the checkbox for selecting .pbix files from local machine not visible
                self.window.central_container.middle_left.chkbox_select_local_files.setVisible(False)  
                logging.info('Hidden: File Selector Checkbox')            
                # Make the checkbox for selecting .pbix disabled for toggling
                self.window.central_container.middle_left.chkbox_select_local_files.setEnabled(False)
                logging.info('Disabled: File Selector Checkbox')

                # Make the file browser button disabled
                self.window.central_container.middle_left.btn_choose_directory.setEnabled(False)
                logging.info('Disabled: Choose Directory Button')
                # Make the file browser button not visible
                self.window.central_container.middle_left.btn_choose_directory.setVisible(False)
                logging.info('Hidden: Choose Directory Button')

            else:
                logging.error('Unexpected Value for BI Tool')
                
        except AttributeError as e:
            self.window.mbox.critical(self.window, 'Attribute Error Occured', f'Error: {e}')
        except TypeError as e:
            self.window.mbox.critical(self.window, 'Type Error Occured', f'Error: {e}')
        except IndexError as e:
            self.window.mbox.critical(self.window, 'Index Error Occured', f'Error: {e}')
        except RuntimeError as e:
            self.window.mbox.critical(self.window, 'Runtime Error Occured', f'Error: {e}')
        except ConnectionError as e:
            self.window.mbox.critical(self.window, 'Connection Error Occured', f'Error {e}')
        except Exception as e:
            self.window.mbox.critical(self.window, 'Runtime Critical Error Occured', f'Error: {e}')        
            