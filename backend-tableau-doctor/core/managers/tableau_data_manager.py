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
                        datasource_id = (
                            datasource_field.datasource.id
                            if datasource_field.datasource
                            else None
                        )
                        field_source = (
                            datasource_field.datasource.name
                            if datasource_field.datasource
                            else None
                        )
                        formula = None
                        # If the field is a calculated field the get the formula
                        if field_type == 'CalculatedField':
                            formula = datasource_field.formula

                        table_name=''
                        column_name=''
                        if len(datasource_field.upstreamColumns) == 0:
                            flat_data_wb.append({
                                'project_id': project_id if project_id else '',
                                'project_name': project_name if project_name else '',
                                'workbook_id': workbook_id if workbook_id else '',
                                'workbook_name': workbook_name if workbook_name else '',
                                'workbook_owner_id': workbook_owner_id if workbook_owner_id else '',
                                'workbook_owner_username': workbook_owner_username if workbook_owner_username else '',
                                'dashboard_id': dashboard_id if dashboard_id else '',
                                'dashboard_name': dashboard_name if dashboard_name else '',
                                'sheet_id': sheet_id if sheet_id else '',
                                'sheet_name':sheet_name if sheet_name else '',
                                'field_id':field_id if field_id else '',
                                'field_name':field_name if field_name else '',
                                'field_type':field_type if field_type else '',
                                'datasource_id':datasource_id if datasource_id else '',
                                'data_source':field_source if field_source else '',
                                'table_name':table_name if table_name else '',
                                'column_name':column_name if column_name else '',
                                'formula':formula,
                                #'Flag': 'Workbook'
                            })
                        else:
                            for field_column in datasource_field.upstreamColumns:
                                column_name = field_column.name
                                for table in field_column.table or []:
                                    table_name = table.name
                                
                                # Append the flattened data for the current field
                                flat_data_wb.append({
                                    'project_id': project_id if project_id else '',
                                    'project_name': project_name if project_name else '',
                                    'workbook_id': workbook_id if workbook_id else '',
                                    'workbook_name': workbook_name if workbook_name else '',
                                    'workbook_owner_id': workbook_owner_id if workbook_owner_id else '',
                                    'workbook_owner_username': workbook_owner_username if workbook_owner_username else '',
                                    'dashboard_id': dashboard_id if dashboard_id else '',
                                    'dashboard_name': dashboard_name if dashboard_name else '',
                                    'sheet_id': sheet_id if sheet_id else '',
                                    'sheet_name':sheet_name if sheet_name else '',
                                    'field_id':field_id if field_id else '',
                                    'field_name':field_name if field_name else '',
                                    'field_type':field_type if field_type else '',
                                    'datasource_id':datasource_id if datasource_id else '',
                                    'data_source':field_source if field_source else '',
                                    'table_name':table_name if table_name else '',
                                    'column_name':column_name if column_name else '',
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
            workbook_id = workbook.id
            workbook_luid = workbook.luid
            workbook_name = workbook.name
            workbook_createdAt = workbook.createdAt
            workbook_updatedAt = workbook.updatedAt
            workbook_tags = ', '.join([t.name for t in workbook.tags]) if workbook.tags else ''
            workbook_description = workbook.description
            project_name = workbook.projectName
            project_id = workbook.projectVizportalUrlId
            sheet_id = None
            sheet_name = None
            dashboard_id = None
            dashboard_name = None

            for embedded_datasource in workbook.embeddedDatasources:
                embedded_datasource_id = embedded_datasource.id
                embedded_datasource_name = embedded_datasource.name
                embedded_datasource_createdAt = embedded_datasource.createdAt
                embedded_datasource_updatedAt = embedded_datasource.updatedAt
                embedded_datasource_hasExtracts = embedded_datasource.hasExtracts

                for dashboard in workbook.dashboards:
                    dashboard_id = dashboard.id
                    dashboard_name = dashboard.name
                    for sheet in dashboard.sheets:
                        sheet_id = sheet.id
                        sheet_name = sheet.name

                # Decide ONCE per embedded datasource
                has_referenced_queries = any(
                    ut.referencedByQueries
                    for ut in embedded_datasource.upstreamTables or []
                )

                # ==========================================================
                # CASE 1: REFERENCED QUERY EXISTS → QUERY-BASED FLATTENING
                # ==========================================================
                if has_referenced_queries:

                    for upstream_table in embedded_datasource.upstreamTables or []:
                        for referenced_query in upstream_table.referencedByQueries or []:

                            query_id = referenced_query.id
                            query_name = referenced_query.name
                            query_text = referenced_query.query

                            flat_query_data.append({
                                'project_id': project_id if project_id else '',
                                'project_name': project_name if project_name else '',
                                'workbook_id': workbook_id if workbook_id else '',
                                'workbook_name': workbook_name if workbook_name else '',
                                'CustomQueryID': query_id if query_id else '',
                                'CustomQuery': query_name if query_name else '',
                                'query': query_text.replace("\r\n", " ") if query_text else '',
                                'Flag': 'Workbook'
                            })

                            for column in referenced_query.columns or []:
                                column_name = column.name
                                used_in_workbook = 'N'

                                for dswb in column.downstreamWorkbooks or []:
                                    if dswb.id == workbook_id:
                                        used_in_workbook = 'Y'
                                        break
                                # NEW: take field info from referencedByFields
                                for ref_field in column.downstreamFields or [None]:
                                    flat_embd_data.append({
                                        'project_id': project_id if project_id else '',
                                        'project_name': project_name if project_name else '',
                                        'workbook_id': workbook_id if workbook_id else '',
                                        'workbook_luid': workbook_luid if workbook_luid else '',
                                        'workbook_name': workbook_name if workbook_name else '',
                                        'workbook_createdAt': workbook_createdAt if workbook_createdAt else '',
                                        'workbook_updatedAt': workbook_updatedAt if workbook_updatedAt else '',
                                        'workbook_tags': workbook_tags if workbook_tags else [],
                                        'workbook_description': workbook_description if workbook_description else '',

                                        'datasource_id': embedded_datasource_id if embedded_datasource_id else '',
                                        'datasource_luid': '',
                                        'datasource_name': embedded_datasource_name if embedded_datasource_name else '',
                                        'created_at': embedded_datasource_createdAt if embedded_datasource_createdAt else '',
                                        'updated_at': embedded_datasource_updatedAt if embedded_datasource_updatedAt else '',
                                        'datasource_project_id': '',
                                        'datasource_project_name': '',
                                        'datasource_tags': '',
                                        'has_extracts': embedded_datasource_hasExtracts if embedded_datasource_hasExtracts is not None else False,
                                        'datasource_type': 'Custom SQL',

                                        # 🔴 Field info intentionally NULL
                                        'field_id': ref_field.id if ref_field else None,
                                        'field_name': ref_field.name if ref_field else None,
                                        'field_type': ref_field.field_type if ref_field else None,
                                        'field_formula': ref_field.formula if ref_field and ref_field.field_type == 'CalculatedField' else None,

                                        'table_name': upstream_table.name if upstream_table else '',
                                        'column_name': column_name if column_name else '',

                                        'sheet_id': sheet_id if sheet_id else '',
                                        'sheet_name': sheet_name if sheet_name else '',
                                        'used_in_workbook': used_in_workbook,
                                        'dashboard_id': dashboard_id if dashboard_id else '',
                                        'dashboard_name': dashboard_name if dashboard_name else '',

                                        'query': query_id,
                                        'Flag': 'Workbook'
                                    })

                # ==========================================================
                # CASE 2: NO REFERENCED QUERY → FIELD / COLUMN-BASED
                # ==========================================================
                else:

                    for field in embedded_datasource.fields or []:
                        field_id = field.id
                        field_name = field.name
                        field_type = field.field_type
                        field_formula = field.formula if field_type == 'CalculatedField' else None

                        for upstream_column in field.upstreamColumns or []:
                            column_name = upstream_column.name

                            for table in upstream_column.table or []:
                                table_name = table.name

                                used_in_workbook = 'N'
                                for dswb in upstream_column.downstreamWorkbooks or []:
                                    if dswb.id == workbook_id:
                                        used_in_workbook = 'Y'
                                        break

                                flat_embd_data.append({
                                    'project_id': project_id if project_id else '',
                                    'project_name': project_name if project_name else '',
                                    'workbook_id': workbook_id if workbook_id else '',
                                    'workbook_luid': workbook_luid if workbook_luid else '',
                                    'workbook_name': workbook_name if workbook_name else '',
                                    'workbook_createdAt': workbook_createdAt if workbook_createdAt else '',
                                    'workbook_updatedAt': workbook_updatedAt if workbook_updatedAt else '',
                                    'workbook_tags': workbook_tags if workbook_tags else '',
                                    'workbook_description': workbook_description if workbook_description else '',

                                    'datasource_id': embedded_datasource_id if embedded_datasource_id else '',
                                    'datasource_luid': '',
                                    'datasource_name': embedded_datasource_name if embedded_datasource_name else '',
                                    'created_at': embedded_datasource_createdAt if embedded_datasource_createdAt else '',
                                    'updated_at': embedded_datasource_updatedAt if embedded_datasource_updatedAt else '',
                                    'datasource_project_id': '',
                                    'datasource_project_name': '',
                                    'datasource_tags': '',
                                    'has_extracts': embedded_datasource_hasExtracts if embedded_datasource_hasExtracts is not None else False,
                                    'datasource_type': 'EmbeddedDatasource',

                                    'field_id': field_id if field_id else '',
                                    'field_name': field_name if field_name else '',
                                    'field_type': field_type if field_type else '',
                                    'field_formula': field_formula if field_formula else '',

                                    'table_name': table_name if table_name else '',
                                    'column_name': column_name if column_name else '',

                                    'sheet_id': sheet_id if sheet_id else '',
                                    'sheet_name': sheet_name if sheet_name else '',
                                    'used_in_workbook': used_in_workbook,
                                    'dashboard_id': dashboard_id if dashboard_id else '',
                                    'dashboard_name': dashboard_name if dashboard_name else '',
             
                                    'query': '',
                                    'Flag': 'Workbook'
                                })
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