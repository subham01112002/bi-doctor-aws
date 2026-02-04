# from pydantic import BaseModel, Field
# from typing import List, Optional

# # -------------------------
# # COMMON MODELS
# # -------------------------

# class Owner(BaseModel):
#     id: Optional[str] = None
#     username: Optional[str] = None


# class Tag(BaseModel):
#     id: Optional[str] = None
#     name: Optional[str] = None


# # -------------------------
# # FIELD SECTION
# # -------------------------

# class FieldTable(BaseModel):
#     name: Optional[str] = None


# class FieldUpstreamColumn(BaseModel):
#     name: Optional[str] = None
#     table: Optional[FieldTable] = None


# class DashboardRef(BaseModel):
#     name: Optional[str] = None


# class SheetWorkbookRef(BaseModel):
#     id: Optional[str] = None
#     name: Optional[str] = None
#     projectName: Optional[str] = None
#     projectVizportalUrlId: Optional[str] = None
#     owner: Optional[Owner] = None


# class DownstreamSheet(BaseModel):
#     name: Optional[str] = None
#     containedInDashboards: Optional[List[DashboardRef]] = []
#     workbook: Optional[SheetWorkbookRef] = None


# class DatasourceField(BaseModel):
#     id: Optional[str] = None
#     name: Optional[str] = None
#     field_type: Optional[str] = Field(None, alias="__typename")
#     formula: Optional[str] = None
#     upstreamColumns: Optional[List[FieldUpstreamColumn]] = []
#     downstreamSheets: Optional[List[DownstreamSheet]] = []


# # -------------------------
# # DOWNSTREAM WORKBOOKS
# # -------------------------

# class WorkbookSheet(BaseModel):
#     id: Optional[str] = None
#     name: Optional[str] = None


# class WorkbookDashboard(BaseModel):
#     id: Optional[str] = None
#     name: Optional[str] = None
#     sheets: Optional[List[WorkbookSheet]] = []


# class DownstreamWorkbook(BaseModel):
#     id: Optional[str] = None
#     name: Optional[str] = None
#     projectName: Optional[str] = None
#     projectVizportalUrlId: Optional[str] = None
#     containsUnsupportedCustomSql: Optional[bool] = None
#     description: Optional[str] = None
#     createdAt: Optional[str] = None
#     owner: Optional[Owner] = None
#     tags: Optional[List[Tag]] = []
#     dashboards: Optional[List[WorkbookDashboard]] = []
#     sheets: Optional[List[WorkbookSheet]] = []


# # -------------------------
# # UPSTREAM TABLES
# # -------------------------

# class QueryWorkbookRef(BaseModel):
#     id: Optional[str] = None


# class QueryColumn(BaseModel):
#     name: Optional[str] = None
#     downstreamWorkbooks: Optional[List[QueryWorkbookRef]] = []


# class ReferencedQuery(BaseModel):
#     id: Optional[str] = None
#     name: Optional[str] = None
#     query: Optional[str] = None
#     columns: Optional[List[QueryColumn]] = []


# class TableColumnWorkbookRef(BaseModel):
#     id: Optional[str] = None
#     name: Optional[str] = None


# class TableColumn(BaseModel):
#     name: Optional[str] = None
#     downstreamWorkbooks: Optional[List[TableColumnWorkbookRef]] = []


# class UpstreamTable(BaseModel):
#     name: Optional[str] = None
#     referencedByQueries: Optional[List[ReferencedQuery]] = []
#     columns: Optional[List[TableColumn]] = []


# # -------------------------
# # MAIN DATASOURCE MODEL
# # -------------------------

# class DatasourceMetadata(BaseModel):
#     id: Optional[str] = None
#     name: Optional[str] = None
#     createdAt: Optional[str] = None
#     updatedAt: Optional[str] = None
#     hasExtracts: Optional[bool] = None

#     fields: Optional[List[DatasourceField]] = []
#     downstreamWorkbooks: Optional[List[DownstreamWorkbook]] = []
#     upstreamTables: Optional[List[UpstreamTable]] = []


# # -------------------------
# # RESPONSE WRAPPER
# # -------------------------

# class DatasourceMetadataResponse(BaseModel):
#     datasources: Optional[List[DatasourceMetadata]] = []


from pydantic import BaseModel, Field
from typing import List, Optional

# -------------------------
# COMMON MODELS
# -------------------------

class Owner(BaseModel):
    id: Optional[str] = None
    username: Optional[str] = None


class Tag(BaseModel):
    name: Optional[str] = None


# -------------------------
# FIELD SECTION
# -------------------------

class FieldTable(BaseModel):
    name: Optional[str] = None


class FieldUpstreamColumn(BaseModel):
    name: Optional[str] = None
    table: Optional[FieldTable] = None


class DashboardRef(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None


class SheetWorkbookRef(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    luid: Optional[str] = None
    projectName: Optional[str] = None
    projectVizportalUrlId: Optional[str] = None
    description: Optional[str] = None
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None
    owner: Optional[Owner] = None
    tags: Optional[List[Tag]] = []


class DownstreamSheet(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    containedInDashboards: Optional[List[DashboardRef]] = []
    workbook: Optional[SheetWorkbookRef] = None


class DatasourceField(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    field_type: Optional[str] = Field(None, alias="__typename")
    formula: Optional[str] = None
    upstreamColumns: Optional[List[FieldUpstreamColumn]] = []
    downstreamSheets: Optional[List[DownstreamSheet]] = []


# -------------------------
# DOWNSTREAM WORKBOOKS
# -------------------------

class WorkbookSheet(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None


class WorkbookDashboard(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    sheets: Optional[List[WorkbookSheet]] = []


class DownstreamWorkbook(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    projectName: Optional[str] = None
    projectVizportalUrlId: Optional[str] = None
    containsUnsupportedCustomSql: Optional[bool] = None
    description: Optional[str] = None
    createdAt: Optional[str] = None
    owner: Optional[Owner] = None
    tags: Optional[List[Tag]] = []
    dashboards: Optional[List[WorkbookDashboard]] = []
    sheets: Optional[List[WorkbookSheet]] = []


# -------------------------
# UPSTREAM TABLES
# -------------------------

class QueryWorkbookRef(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None


class QueryColumn(BaseModel):
    name: Optional[str] = None
    downstreamWorkbooks: Optional[List[QueryWorkbookRef]] = []


class ReferencedQuery(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    query: Optional[str] = None
    columns: Optional[List[QueryColumn]] = []


class TableColumnWorkbookRef(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None


class TableColumn(BaseModel):
    name: Optional[str] = None
    downstreamWorkbooks: Optional[List[TableColumnWorkbookRef]] = []


class UpstreamTable(BaseModel):
    name: Optional[str] = None
    referencedByQueries: Optional[List[ReferencedQuery]] = []
    #columns: Optional[List[TableColumn]] = []


# -------------------------
# MAIN DATASOURCE MODEL
# -------------------------

class DatasourceMetadata(BaseModel):
    id: Optional[str] = None
    luid: Optional[str] = None
    name: Optional[str] = None
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None
    hasExtracts: Optional[bool] = None
    field_type: Optional[str] = Field(None, alias="__typename")
    projectName: Optional[str] = None
    projectVizportalUrlId: Optional[str] = None
    tags: Optional[List[Tag]] = []

    fields: Optional[List[DatasourceField]] = []
    #downstreamWorkbooks: Optional[List[DownstreamWorkbook]] = []
    upstreamTables: Optional[List[UpstreamTable]] = []


# -------------------------
# RESPONSE WRAPPER
# -------------------------

class DatasourceMetadataResponse(BaseModel):
    publishedDatasources: Optional[List[DatasourceMetadata]] = []

 