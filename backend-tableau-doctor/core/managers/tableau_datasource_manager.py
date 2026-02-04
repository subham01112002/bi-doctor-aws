import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class TableauDatasourceDataManager:
    """
    Flattens Tableau Datasource Metadata returned from GraphQL
    """

    def __init__(self, datasource_metadata_response):
        self.datasource_metadata_response = datasource_metadata_response

    # -------------------------------------------------------
    # FLATTEN DATASOURCE → FIELDS → SHEETS → WORKBOOKS
    # -------------------------------------------------------
    def get_flat_datasource_details(self):
        flat_datasource_data = []
        
        for ds in self.datasource_metadata_response.publishedDatasources:
            # Datasource level attributes
            ds_project_id = ds.projectVizportalUrlId
            ds_project = ds.projectName
            datasource_id = ds.id
            datasource_name = ds.name
            created_date_ds = ds.createdAt
            updated_date_ds = ds.updatedAt
            contains_extract = ds.hasExtracts
            tags_ds = ", ".join([t.name for t in ds.tags]) if ds.tags else ""
            datasource_type = ds.field_type
            
            # Build table->column mapping for custom query tracking
            table_column_query_map = {}
            for upstream_table in ds.upstreamTables:
                table_name = upstream_table.name
                if upstream_table.referencedByQueries and len(upstream_table.referencedByQueries) > 0:
                    for query in upstream_table.referencedByQueries:
                        query_id = query.id
                        for col in query.columns:
                            col_name = col.name
                            table_column_query_map[(table_name, col_name)] = query_id
            
            # Iterate through fields in the datasource
            for field in ds.fields:
                field_id = field.id
                field_name = field.name
                field_type = field.field_type
                formula = None
                
                # If it's a calculated field, get the formula
                if field_type == 'CalculatedField':
                    formula = field.formula
                
                # Get upstream columns info
                upstream_columns_info = []
                if field.upstreamColumns and len(field.upstreamColumns) > 0:
                    for upstream_col in field.upstreamColumns:
                        col_name = upstream_col.name
                        table_name = upstream_col.table.name if upstream_col.table else ""
                        # Check if this column comes from a custom query
                        query_id = table_column_query_map.get((table_name, col_name), "")
                        
                        upstream_columns_info.append({
                            'column': col_name,
                            'table': table_name,
                            'query': query_id
                        })
                
                # Get downstream sheets info
                if field.downstreamSheets and len(field.downstreamSheets) > 0:
                    for sheet in field.downstreamSheets:
                        sheet_id = sheet.id
                        sheet_name = sheet.name
                        used_in_sheet = "Y"
                        
                        # Get workbook info (this is at sheet level, not dashboard level)
                        workbook = sheet.workbook
                        workbook_id = workbook.id
                        workbook_name = workbook.name
                        workbook_luid = workbook.luid
                        created_date_wb = workbook.createdAt
                        updated_date_wb = workbook.updatedAt
                        tags_wb = ", ".join([t.name for t in workbook.tags]) if workbook.tags else ""
                        description = workbook.description if workbook.description else ""
                        wb_project = workbook.projectName
                        wb_project_id = workbook.projectVizportalUrlId
                        
                        # Check if sheet is in dashboards
                        if sheet.containedInDashboards and len(sheet.containedInDashboards) > 0:
                            for dashboard in sheet.containedInDashboards:
                                dashboard_id = dashboard.id
                                dashboard_name = dashboard.name
                                
                                # Create rows for each upstream column (with dashboard info)
                                if upstream_columns_info:
                                    for col_info in upstream_columns_info:
                                        flat_datasource_data.append({
                                            "DSProject ID": ds_project_id,
                                            "DSProject": ds_project,
                                            "Datasource ID": datasource_id,
                                            "Datasource": datasource_name,
                                            "CreatedDate": created_date_ds,
                                            "UpdatedDate": updated_date_ds,
                                            "ContainsExtract": contains_extract,
                                            "Tags": tags_ds,
                                            "DataSorceType": datasource_type,
                                            "FieldID": field_id,
                                            "FieldName": field_name,
                                            "FieldType": field_type,
                                            "Formula": formula,
                                            "Column": col_info['column'],
                                            "Table": col_info['table'],
                                            "Sheet ID": sheet_id,
                                            "Sheet": sheet_name,
                                            "UsedInSheet": used_in_sheet,
                                            "Dashboard ID": dashboard_id,
                                            "Dashboard": dashboard_name,
                                            "Workbook ID": workbook_id,
                                            "Workbook": workbook_name,
                                            "WorkbookLUID": workbook_luid,
                                            "WBCreatedDate": created_date_wb,
                                            "WBUpdatedDate": updated_date_wb,
                                            "WBTags": tags_wb,
                                            "Description": description,
                                            "WBProject": wb_project,
                                            "WBProjectID": wb_project_id,
                                            "CustomQueryID": col_info['query'],
                                            "Flag": "Datasource"
                                        })
                                else:
                                    # No upstream columns but in dashboard
                                    flat_datasource_data.append({
                                        "DSProject ID": ds_project_id,
                                        "DSProject": ds_project,
                                        "Datasource ID": datasource_id,
                                        "Datasource": datasource_name,
                                        "CreatedDate": created_date_ds,
                                        "UpdatedDate": updated_date_ds,
                                        "ContainsExtract": contains_extract,
                                        "Tags": tags_ds,
                                        "DataSorceType": datasource_type,
                                        "FieldID": field_id,
                                        "FieldName": field_name,
                                        "FieldType": field_type,
                                        "Formula": formula,
                                        "Column": "",
                                        "Table": "",
                                        "Sheet ID": sheet_id,
                                        "Sheet": sheet_name,
                                        "UsedInSheet": used_in_sheet,
                                        "Dashboard ID": dashboard_id,
                                        "Dashboard": dashboard_name,
                                        "Workbook ID": workbook_id,
                                        "Workbook": workbook_name,
                                        "WorkbookLUID": workbook_luid,
                                        "WBCreatedDate": created_date_wb,
                                        "WBUpdatedDate": updated_date_wb,
                                        "WBTags": tags_wb,
                                        "Description": description,
                                        "WBProject": wb_project,
                                        "WBProjectID": wb_project_id,
                                        "CustomQueryID": "",
                                        "Flag": "Datasource"
                                    })
                        else:
                            # Sheet not in any dashboard
                            if upstream_columns_info:
                                for col_info in upstream_columns_info:
                                    flat_datasource_data.append({
                                        "DSProject ID": ds_project_id,
                                        "DSProject": ds_project,
                                        "Datasource ID": datasource_id,
                                        "Datasource": datasource_name,
                                        "CreatedDate": created_date_ds,
                                        "UpdatedDate": updated_date_ds,
                                        "ContainsExtract": contains_extract,
                                        "Tags": tags_ds,
                                        "DataSorceType": datasource_type,
                                        "FieldID": field_id,
                                        "FieldName": field_name,
                                        "FieldType": field_type,
                                        "Formula": formula,
                                        "Column": col_info['column'],
                                        "Table": col_info['table'],
                                        "Sheet ID": sheet_id,
                                        "Sheet": sheet_name,
                                        "UsedInSheet": used_in_sheet,
                                        "Dashboard ID": "",
                                        "Dashboard": "",
                                        "Workbook ID": workbook_id,
                                        "Workbook": workbook_name,
                                        "WorkbookLUID": workbook_luid,
                                        "WBCreatedDate": created_date_wb,
                                        "WBUpdatedDate": updated_date_wb,
                                        "WBTags": tags_wb,
                                        "Description": description,
                                        "WBProject": wb_project,
                                        "WBProjectID": wb_project_id,
                                        "CustomQueryID": col_info['query'],
                                        "Flag": "Datasource"
                                    })
                            else:
                                # No upstream columns and not in dashboard
                                flat_datasource_data.append({
                                    "DSProject ID": ds_project_id,
                                    "DSProject": ds_project,
                                    "Datasource ID": datasource_id,
                                    "Datasource": datasource_name,
                                    "CreatedDate": created_date_ds,
                                    "UpdatedDate": updated_date_ds,
                                    "ContainsExtract": contains_extract,
                                    "Tags": tags_ds,
                                    "DataSorceType": datasource_type,
                                    "FieldID": field_id,
                                    "FieldName": field_name,
                                    "FieldType": field_type,
                                    "Formula": formula,
                                    "Column": "",
                                    "Table": "",
                                    "Sheet ID": sheet_id,
                                    "Sheet": sheet_name,
                                    "UsedInSheet": used_in_sheet,
                                    "Dashboard ID": "",
                                    "Dashboard": "",
                                    "Workbook ID": workbook_id,
                                    "Workbook": workbook_name,
                                    "WorkbookLUID": workbook_luid,
                                    "WBCreatedDate": created_date_wb,
                                    "WBUpdatedDate": updated_date_wb,
                                    "WBTags": tags_wb,
                                    "Description": description,
                                    "WBProject": wb_project,
                                    "WBProjectID": wb_project_id,
                                    "CustomQueryID": "",
                                    "Flag": "Datasource"
                                })
                else:
                    # Field has no downstream sheets - still include datasource/field info
                    if upstream_columns_info:
                        for col_info in upstream_columns_info:
                            flat_datasource_data.append({
                                "DSProject ID": ds_project_id,
                                "DSProject": ds_project,
                                "Datasource ID": datasource_id,
                                "Datasource": datasource_name,
                                "CreatedDate": created_date_ds,
                                "UpdatedDate": updated_date_ds,
                                "ContainsExtract": contains_extract,
                                "Tags": tags_ds,
                                "DataSorceType": datasource_type,
                                "FieldID": field_id,
                                "FieldName": field_name,
                                "FieldType": field_type,
                                "Formula": formula,
                                "Column": col_info['column'],
                                "Table": col_info['table'],
                                "Sheet ID": "",
                                "Sheet": "",
                                "UsedInSheet": "N",
                                "Dashboard ID": "",
                                "Dashboard": "",
                                "Workbook ID": "",
                                "Workbook": "",
                                "WorkbookLUID": "",
                                "WBCreatedDate": "",
                                "WBUpdatedDate": "",
                                "WBTags": "",
                                "Description": "",
                                "WBProject": "",
                                "WBProjectID": "",
                                "CustomQueryID": col_info['query'],
                                "Flag": "Datasource"
                            })
                    else:
                        # No upstream columns and no downstream sheets
                        flat_datasource_data.append({
                            "DSProject ID": ds_project_id,
                            "DSProject": ds_project,
                            "Datasource ID": datasource_id,
                            "Datasource": datasource_name,
                            "CreatedDate": created_date_ds,
                            "UpdatedDate": updated_date_ds,
                            "ContainsExtract": contains_extract,
                            "Tags": tags_ds,
                            "DataSorceType": datasource_type,
                            "FieldID": field_id,
                            "FieldName": field_name,
                            "FieldType": field_type,
                            "Formula": formula,
                            "Column": "",
                            "Table": "",
                            "Sheet ID": "",
                            "Sheet": "",
                            "UsedInSheet": "N",
                            "Dashboard ID": "",
                            "Dashboard": "",
                            "Workbook ID": "",
                            "Workbook": "",
                            "WorkbookLUID": "",
                            "WBCreatedDate": "",
                            "WBUpdatedDate": "",
                            "WBTags": "",
                            "Description": "",
                            "WBProject": "",
                            "WBProjectID": "",
                            "CustomQueryID": "",
                            "Flag": "Datasource"
                        })
        
        logging.info("Flattened: Datasource Details at Datasource Level")
        return flat_datasource_data 
    
    
    def get_flat_ds_custom_queries(self):
        flat_query_data = []
        seen = set() # deduplicate tracker

        for ds in self.datasource_metadata_response.publishedDatasources:
            project_id = ds.projectVizportalUrlId
            project_name = ds.projectName
            for upstream_tables in ds.upstreamTables:
                # ONLY TABLES WITH CUSTOM QUERIES
                if upstream_tables.referencedByQueries:
                    for query in upstream_tables.referencedByQueries:
                        query_text = (
                            query.query.replace("\r\n", " ")
                            if query.query else None
                        )
                        for col in query.columns:
                            for wb in col.downstreamWorkbooks:
                                key = (query.id, wb.id)  # deduplicate key

                                if key in seen:
                                    continue

                                seen.add(key)
                                flat_query_data.append({
                                    "project_id": project_id,
                                    "project_name": project_name,
                                    "workbook_id": wb.id,
                                    "workbook_name": wb.name,
                                    "custom_query_id": query.id,
                                    "custom_query_name": query.name,
                                    "query": query_text,
                                    "Flag": "Datasource"
                                })

        logging.info("Flattened: Custom queries used in Datasources")
        return flat_query_data


    # -------------------------------------------------------
    # SUMMARY COUNTS (LIKE WORKBOOK SUMMARY)
    # -------------------------------------------------------
    # def get_datasource_counts(self):
    #     summary = []

    #     for ds in self.datasource_metadata_response.datasources:
    #         fields = set()
    #         tables = set()
    #         columns = set()
    #         queries = set()
    #         workbooks = set()

    #         for field in ds.fields:
    #             fields.add(field.name)

    #         for table in ds.upstreamTables:
    #             tables.add(table.name)

    #             if table.referencedByQueries:
    #                 for q in table.referencedByQueries:
    #                     queries.add(q.query)
    #                     for col in q.columns:
    #                         columns.add(col.name)
    #             else:
    #                 for col in table.columns:
    #                     columns.add(col.name)

    #         for wb in ds.downstreamWorkbooks:
    #             workbooks.add(wb.name)

    #         summary.append({
    #             "Datasource": ds.name,
    #             "Fields": len(fields),
    #             "Tables": len(tables),
    #             "Columns": len(columns),
    #             "Custom Queries": len(queries),
    #             "Used In Workbooks": len(workbooks)
    #         })

    #     return summary
