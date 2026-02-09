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
            # Datasource level attributes - ensure proper string conversion
            ds_project_id = str(ds.projectVizportalUrlId) if ds.projectVizportalUrlId else ""
            ds_project = str(ds.projectName) if ds.projectName else ""
            datasource_id = str(ds.id) if ds.id else ""
            datasource_luid = str(ds.luid) if ds.luid else ""
            datasource_name = str(ds.name) if ds.name else ""
            created_date_ds = str(ds.createdAt) if ds.createdAt else ""
            updated_date_ds = str(ds.updatedAt) if ds.updatedAt else ""
            contains_extract = str(ds.hasExtracts) if ds.hasExtracts is not None else ""
            tags_ds = ", ".join([str(t.name) for t in ds.tags]) if ds.tags else ""
            datasource_type = str(ds.field_type) if ds.field_type else ""
            
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
                field_id = str(field.id) if field.id else ""
                field_name = str(field.name) if field.name else ""
                field_type = str(field.field_type) if field.field_type else ""
                formula = str(field.formula) if field.formula else ""
                
                # Get upstream columns and their tables' downstream sheets
                if field.upstreamColumns and len(field.upstreamColumns) > 0:
                    for upstream_col in field.upstreamColumns:
                        col_name = str(upstream_col.name) if upstream_col.name else ""
                        table_name = str(upstream_col.table.name) if upstream_col.table and upstream_col.table.name else ""
                        # Check if this column comes from a custom query
                        query_id = str(table_column_query_map.get((table_name, col_name), ""))
                        
                        # Check if the table exists and has downstream sheets
                        if upstream_col.table and upstream_col.table.downstreamSheets and len(upstream_col.table.downstreamSheets) > 0:
                            # Loop through each sheet that uses this table
                            for sheet in upstream_col.table.downstreamSheets:
                                sheet_id = str(sheet.id) if sheet.id else ""
                                sheet_name = str(sheet.name) if sheet.name else ""
                                used_in_sheet = "Y"
                                
                                # ===== CRITICAL FIX: Check if workbook exists =====
                                if sheet.workbook:
                                    workbook = sheet.workbook
                                    workbook_id = str(workbook.id) if workbook.id else ""
                                    workbook_name = str(workbook.name) if workbook.name else ""
                                    workbook_luid = str(workbook.luid) if workbook.luid else ""
                                    created_date_wb = str(workbook.createdAt) if workbook.createdAt else ""
                                    updated_date_wb = str(workbook.updatedAt) if workbook.updatedAt else ""
                                    tags_wb = ", ".join([str(t.name) for t in workbook.tags]) if workbook.tags else ""
                                    description = str(workbook.description) if workbook.description else ""
                                    wb_project = str(workbook.projectName) if workbook.projectName else ""
                                    wb_project_id = str(workbook.projectVizportalUrlId) if workbook.projectVizportalUrlId else ""
                                else:
                                    # Workbook is None - use empty values
                                    logging.warning(f"Sheet {sheet_id} ({sheet_name}) has no workbook information")
                                    workbook_id = workbook_name = workbook_luid = ""
                                    created_date_wb = updated_date_wb = tags_wb = description = ""
                                    wb_project = wb_project_id = ""
                                
                                # Check if sheet is in dashboards
                                if sheet.containedInDashboards and len(sheet.containedInDashboards) > 0:
                                    # Loop through each dashboard containing this sheet
                                    for dashboard in sheet.containedInDashboards:
                                        dashboard_id = str(dashboard.id) if dashboard.id else ""
                                        dashboard_name = str(dashboard.name) if dashboard.name else ""
                                        
                                        flat_datasource_data.append({
                                            "DSProject ID": ds_project_id,
                                            "DSProject": ds_project,
                                            "Datasource ID": datasource_id,
                                            "datasource_luid":datasource_luid,
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
                                            "Column": col_name,
                                            "Table": table_name,
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
                                            "CustomQueryID": query_id,
                                            "Flag": "Datasource"
                                        })
                                else:
                                    # Sheet not in any dashboard
                                    flat_datasource_data.append({
                                        "DSProject ID": ds_project_id,
                                        "DSProject": ds_project,
                                        "Datasource ID": datasource_id,
                                        "datasource_luid":datasource_luid,
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
                                        "Column": col_name,
                                        "Table": table_name,
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
                                        "CustomQueryID": query_id,
                                        "Flag": "Datasource"
                                    })
                        else:
                            # Upstream column exists but its table has no downstream sheets
                            flat_datasource_data.append({
                                "DSProject ID": ds_project_id,
                                "DSProject": ds_project,
                                "Datasource ID": datasource_id,
                                "datasource_luid": datasource_luid,
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
                                "Column": col_name,
                                "Table": table_name,
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
                                "CustomQueryID": query_id,
                                "Flag": "Datasource"
                            })
                else:
                    # Field has no upstream columns
                    flat_datasource_data.append({
                        "DSProject ID": ds_project_id,
                        "DSProject": ds_project,
                        "Datasource ID": datasource_id,
                        "datasource_luid": datasource_luid,
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
