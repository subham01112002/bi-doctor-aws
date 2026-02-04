from pydantic import BaseModel, Field
from typing import List, Optional


class Table(BaseModel):
    # Represents a data source with a name.
    name: Optional[str] = None


class Datasource(BaseModel):
    # Represents a data source with a name.
    id: Optional[str] = None
    name: Optional[str] = None


class UpstreamColumn(BaseModel):
    name: Optional[str] = None
    table: Optional[Table] = None


class DatasourceField(BaseModel):
    # Represents a data source field with a name, data source, and optional formula.
    id: Optional[str] = None
    name: Optional[str] = None
    datasource: Optional[Datasource] = None
    upstreamColumns: Optional[List[UpstreamColumn]] = []
    field_type: Optional[str] = Field(None, alias="__typename")
    formula: Optional[str] = None


class Sheet(BaseModel):
    # Represents a sheet with a name, ID, and data source fields.
    name: Optional[str] = None
    id: Optional[str] = None
    datasourceFields: Optional[List[DatasourceField]] = []


class Dashboard(BaseModel):
    # Represents a dashboard with a name, ID, and sheets.
    name: Optional[str] = None
    id: Optional[str] = None
    sheets: Optional[List[Sheet]] = []


class QueryDownStreamWorkbook(BaseModel):
    id: Optional[str] = None


class QueryColumn(BaseModel):
    # Represents a query column with a name.
    name: Optional[str] = None
    downstreamWorkbooks: Optional[List[QueryDownStreamWorkbook]] = []


class ReferencedQuery(BaseModel):
    # Represents a referenced query with a query and columns.
    id: Optional[str] = None
    name: Optional[str] = None
    query: Optional[str] = None
    columns: Optional[List[QueryColumn]] = []


class TableColumnDownStreamWorkbook(BaseModel):
    id: Optional[str] = None


class TableColumn(BaseModel):
    # Represents a table column with a name.
    name: Optional[str] = None   
    downstreamWorkbooks: Optional[List[TableColumnDownStreamWorkbook]] = []


class UpstreamTable(BaseModel):
    # Represents an upstream table with a name, referenced queries, and columns.
    name: Optional[str] = None
    referencedByQueries: Optional[List[ReferencedQuery]] = []
    columns: Optional[List[TableColumn]] = []


class EmbeddedDatasource(BaseModel):
    # Represents an embedded data source with a name and upstream tables.
    id: Optional[str] = None
    name: Optional[str] = None
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None
    hasExtracts: Optional[bool] = None
    fields: Optional[List[DatasourceField]] = []
    upstreamTables: Optional[List[UpstreamTable]] = []


class Owner(BaseModel):
    id: Optional[str] = None
    username: Optional[str] = None


class Workbook(BaseModel):
    # Represents a workbook with a name, ID, project name, project URL ID, dashboards, and embedded data sources.
    name: Optional[str] = None
    id: Optional[str] = None
    luid: Optional[str] = None
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = []
    projectName: Optional[str] = None
    #projectLuid: Optional[str] = None
    projectVizportalUrlId: Optional[str] = None
    dashboards: Optional[List[Dashboard]] = []
    embeddedDatasources: Optional[List[EmbeddedDatasource]] = []
    owner: Optional[Owner] = None
    field: Optional[DatasourceField] = None
    datasource: Optional[Datasource] = None
    

class WorkbooksResponse(BaseModel):
    # Represents a response with a list of workbooks.
    workbooks: Optional[List[Workbook]] = []
    
