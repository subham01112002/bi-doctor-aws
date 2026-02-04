"""
TableauDataManager Class

This module defines the `TableauDataManager` class, which facilitates interactions 
with Tableau data by managing authentication and querying operations.

Classes:
- TableauDataManager: Responsible for managing Tableau data interactions.

Methods:
- __init__(self, auth: TableauAuthClient): Initializes the TableauDataManager with an 
  authentication client and query client, and handles signing in to Tableau.
  
- dropdown_loader(self): Executes a query to load workbook data from Tableau and 
  returns the data as a `DropdownLoaderResponse` object. This method handles 
  exceptions related to data loading and logs any errors.

- get_full_workbook_data(self, workbook_ids): Accepts a list of workbook IDs, queries 
  workbook metadata from Tableau, and parses the response into a `WorkbooksResponse` 
  object. Handles a variety of exceptions, logs them, and re-raises for higher-level handling.

Logging:
- Each method includes logging for successful operations and any errors, ensuring that 
  both authentication and data retrieval steps are traceable.
"""

import logging

# Set up basic logging configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class TableauDataManager:
    def __init__(self, full_workbook_data):
        self.full_workbook_data = full_workbook_data

    def get_flat_wb_data(self):
        flat_data_wb = []
        print(len(self.full_workbook_data.workbooks))
        #Iterate through each workbook in the full workbook data
        for workbook in self.full_workbook_data.workbooks:
            project_id = workbook.projectVizportalUrlId
            project_name = workbook.projectName
            workbook_id = workbook.id
            workbook_name = workbook.name
            # workbook_owner_id = workbook.owner.id if workbook.owner else None
            # workbook_owner_username = workbook.owner.username if workbook.owner else None
            workbook_owner_id = workbook.owner.id
            workbook_owner_username = workbook.owner.username
            print(workbook.dashboards)
            for dashboard in workbook.dashboards:
                dashboard_id = dashboard.id
                dashboard_name = dashboard.name
                for sheet in dashboard.sheets:
                    sheet_id = sheet.id
                    sheet_name = sheet.name
                    for datasource_field in sheet.datasourceFields:
                        field_id = datasource_field.id
                        field_name = datasource_field.name
                        field_type = datasource_field.field_type
                        datasource_id= datasource_field.datasource.id
                        field_source = datasource_field.datasource.name
                        formula = None
                        # If the field is a calculated field the get the formula
                        if field_type == 'CalculatedField':
                            formula = datasource_field.formula

                        table_name=''
                        column_name=''
                        if len(datasource_field.upstreamColumns) == 0:
                            flat_data_wb.append({
                                'project_id': project_id,
                                'project_name': project_name,
                                'workbook_id': workbook_id,
                                'workbook_name': workbook_name,
                                'workbook_owner_id': workbook_owner_id,
                                'workbook_owner_username': workbook_owner_username,
                                'dashboard_id': dashboard_id,
                                'dashboard_name': dashboard_name,
                                'sheet_id': sheet_id,
                                'sheet_name':sheet_name,
                                'field_id':field_id,
                                'field_name':field_name,
                                'field_type':field_type,
                                'datasource_id':datasource_id,
                                'data_source':field_source,
                                'table_name':table_name,
                                'column_name':column_name,
                                'formula':formula,
                                #'Flag': 'Workbook'
                            })
                        else:
                            for field_column in datasource_field.upstreamColumns:
                                column_name = field_column.name
                                table_name = field_column.table.name
                                
                                # Append the flattened data for the current field
                                flat_data_wb.append({
                                    'project_id': project_id,
                                    'project_name': project_name,
                                    'workbook_id': workbook_id,
                                    'workbook_name': workbook_name,
                                    'workbook_owner_id': workbook_owner_id,
                                    'workbook_owner_username': workbook_owner_username,
                                    'dashboard_id': dashboard_id,
                                    'dashboard_name': dashboard_name,
                                    'sheet_id': sheet_id,
                                    'sheet_name':sheet_name,
                                    'field_id':field_id,
                                    'field_name':field_name,
                                    'field_type':field_type,
                                    'datasource_id':datasource_id,
                                    'data_source':field_source,
                                    'table_name':table_name,
                                    'column_name':column_name,
                                    'formula':formula,
                                    #'Flag': 'Workbook'
                                })


        logging.info('Flattened: Workbook Details')

        return flat_data_wb
    
    def get_flat_embd_data(self):
        flat_embd_data = []
        flat_query_data = []
        print(len(self.full_workbook_data.workbooks))
        for workbook in self.full_workbook_data.workbooks:
            workbook_name = workbook.name
            workbook_id = workbook.id
            workbook_luid = workbook.luid
            workbook_createdAt = workbook.createdAt
            workbook_updatedAt = workbook.updatedAt
            workbook_tags = ', '.join([tag.name for tag in workbook.tags]) if workbook.tags else ''
            workbook_description = workbook.description
            project_name = workbook.projectName
            project_id = workbook.projectVizportalUrlId
            dashboard_id = None
            dashboard_name = None
            sheet_id = None
            sheet_name = None
            for dashboard in workbook.dashboards:
                dashboard_id = dashboard.id
                dashboard_name = dashboard.name
                for sheet in dashboard.sheets:
                    sheet_id = sheet.id
                    sheet_name = sheet.name
            for embedded_datasource in workbook.embeddedDatasources:
                embedded_datasource_id = embedded_datasource.id
                embedded_datasource_name = embedded_datasource.name
                embedded_datasource_createdAt = embedded_datasource.createdAt
                embedded_datasource_updatedAt = embedded_datasource.updatedAt
                embedded_datasource_hasExtracts = embedded_datasource.hasExtracts
                for field in embedded_datasource.fields:
                    field_id = field.id
                    field_name = field.name
                    field_type = field.field_type
                    field_formula = None
                    if field_type == 'CalculatedField':
                        field_formula = field.formula
                for upstream_table in embedded_datasource.upstreamTables:
                    upstream_table_name = upstream_table.name
                    
                    # Check if the upstream table is referenced by any queries
                    if len(upstream_table.referencedByQueries) == 0:
                        # If not referenced, add each column to flat_embd_data
                        for column in upstream_table.columns:
                            column_name = column.name
                            used_in_workbook = 'N'
                            
                            for dswb in column.downstreamWorkbooks:
                                if workbook_id == dswb.id:
                                    used_in_workbook = 'Y'

                            flat_embd_data.append({
                                'project_id': project_id,
                                'project_name': project_name,
                                'workbook_id': workbook_id,
                                'workbook_luid': workbook_luid,
                                'workbook_name':workbook_name,
                                'workbook_createdAt':workbook_createdAt,
                                'workbook_updatedAt':workbook_updatedAt,
                                'workbook_tags':workbook_tags,
                                'workbook_description':workbook_description,
                                'datasource_id':embedded_datasource_id,
                                'datasource_name':embedded_datasource_name,
                                'created_at':embedded_datasource_createdAt,
                                'updated_at':embedded_datasource_updatedAt,
                                'datasource_project_id':'',
                                'datasource_project_name':'',
                                'datasource_tags':'',
                                'has_extracts':embedded_datasource_hasExtracts,
                                'datasource_type':'EmbeddedDatasource',
                                'field_id':field_id,
                                'field_name':field_name,
                                'field_type':field_type,
                                'field_formula':field_formula,
                                'table_name':upstream_table_name,
                                'column_name':column_name,
                                'sheet_id':sheet_id,
                                'sheet_name':sheet_name,
                                'used_in_workbook':used_in_workbook,
                                'dashboard_id':dashboard_id,
                                'dashboard_name':dashboard_name,
                                'query':'',  # No query associated
                                'Flag': 'Workbook'
                            })
                    else:
                        # If referenced, process each query referencing the table
                        i = 1
                        for referenced_by_query in upstream_table.referencedByQueries:
                            query = referenced_by_query.query
                            query_name = referenced_by_query.name
                            query_id = referenced_by_query.id
                            flat_query_data.append({
                                'project_id': project_id,
                                'project_name': project_name,
                                'workbook_id': workbook_id,
                                'workbook_name':workbook_name,
                                'CustomQueryID':query_id,
                                'CustomQuery':query_name,
                                'query':query.replace("\r\n", " "),
                                "Flag": 'Workbook'
                            })
                            # Add each column associated with the query to the flat_embedded_data
                            for column in referenced_by_query.columns:
                                column_name = column.name
                                used_in_workbook = 'N'
                                
                                for dswb in column.downstreamWorkbooks:
                                    if workbook_id == dswb.id:
                                        used_in_workbook = 'Y'

                                flat_embd_data.append({
                                    'project_id': project_id,
                                    'project_name': project_name,
                                    'workbook_id': workbook_id,
                                    'workbook_luid': workbook_luid,
                                    'workbook_name':workbook_name,
                                    'workbook_createdAt':workbook_createdAt,
                                    'workbook_updatedAt':workbook_updatedAt,
                                    'workbook_tags':workbook_tags,
                                    'workbook_description':workbook_description,
                                    'datasource_id':embedded_datasource_id,
                                    'datasource_name':embedded_datasource_name,
                                    'created_at':embedded_datasource_createdAt,
                                    'updated_at':embedded_datasource_updatedAt,
                                    'datasource_project_id':'',
                                    'datasource_project_name':'',
                                    'datasource_tags':'',
                                    'has_extracts':embedded_datasource_hasExtracts,
                                    'datasource_type':'EmbeddedDatasource',
                                    'field_id':field_id,
                                    'field_name':field_name,
                                    'field_type':field_type,
                                    'field_formula':field_formula,
                                    'table_name':upstream_table_name,
                                    'column_name':column_name,
                                    'sheet_id':sheet_id,
                                    'sheet_name':sheet_name,
                                    'used_in_workbook':used_in_workbook,
                                    'dashboard_id':dashboard_id,
                                    'dashboard_name':dashboard_name,
                                    'query':query_id,
                                    'Flag': 'Workbook'
                                })
                            i += 1 # Increment query index for the next query

        logging.info('Flattened: Embedded Data Source Details')
        logging.info('Flattened: Query Details')
        return flat_embd_data, flat_query_data
    
    def get_workbook_counts(self):
        """Get the unique counts from the Workbook Details Sheets"""
        summary_list = []
        print(len(self.full_workbook_data.workbooks))
        # Iterate through each workbook in the full workbook data
        for workbook in self.full_workbook_data.workbooks:
            workbook_name = workbook.name
            dashboards = set()
            sheets = set()
            fields = set()
            field_types = set()
            formulas = set()
            print(len(workbook.dashboards))
            # Process each dashboard in the workbook
            for dashboard in workbook.dashboards:
                dashboards.add(dashboard.name)
                print(len(dashboard.sheets))
                # Process each sheet in the dashboard
                for sheet in dashboard.sheets:
                    sheets.add(sheet.name)
                    print(len(sheet.datasourceFields))
                    # Process each datasource field in the sheet
                    for datasource_field in sheet.datasourceFields:
                        fields.add(datasource_field.name)
                        field_types.add(datasource_field.field_type)
 
                        # Check if calculated field
                        if datasource_field.field_type == 'CalculatedField':
                            formulas.add(datasource_field.formula)
            print(dashboards)
            print(sheets)
            print(fields)
            print(field_types)
            # Append workbook-level summary
            summary_list.append({
                "Workbook": workbook_name,
                "Dashboards": len(dashboards),
                "Sheets": len(sheets),
                "Fields": len(fields),
                "Field Types": len(field_types),
                "Formula Fields": len(formulas)
            })
 
        return summary_list
 
    def get_datasource_counts(self):
        """Get the unique counts from the Datasource Details and Query Details Sheet"""
        summary_list = []
 
        for workbook in self.full_workbook_data.workbooks:
            datasources = set()
            tables = set()
            columns = set()
            custom_table_columns = set()
            queries = set()
 
            # Process embedded data sources
            for embedded_datasource in workbook.embeddedDatasources:
                datasources.add(embedded_datasource.name)
 
                # Process upstream tables
                for upstream_table in embedded_datasource.upstreamTables:
                    tables.add(upstream_table.name)
 
                    if not upstream_table.referencedByQueries:
                        for column in upstream_table.columns:
                            columns.add(column.name)
                    else:
                        for referenced_by_query in upstream_table.referencedByQueries:
                            queries.add(referenced_by_query.query)
                            for column in referenced_by_query.columns:
                                custom_table_columns.add(column.name)
 
            # Append workbook-level summary
            summary_list.append({
                "Workbook": workbook.name,
                "Datasources": len(datasources),
                "Tables": len(tables),
                "Columns": len(columns),
                "Custom Columns": len(custom_table_columns),
                "Custom Queries": len(queries)
            })
 
        return summary_list
    
