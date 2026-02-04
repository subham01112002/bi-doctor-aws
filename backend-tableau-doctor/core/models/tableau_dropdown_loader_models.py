"""
This module defines Pydantic data models for parsing and validating
the JSON response from a Tableau API that provides workbook details.
"""

from pydantic import BaseModel
from typing import List,Optional

'''class Workbook(BaseModel):
    name: str  # The name of the workbook
    id: str  # Unique identifier for the workbook
    projectName: str  # Name of the project the workbook belongs to
    projectVizportalUrlId: str  # URL ID for the project in the Tableau viz portal
'''
class Workbook(BaseModel):
    name: Optional[str] = None  # The name of the workbook, can be a string or None
    id: Optional[str] = None  # Unique identifier for the workbook
    luid: Optional[str] = None  # Unique LUID for the workbook, can be a string or None
    projectName: Optional[str] = None  # Name of the project, can be a string or None
    projectVizportalUrlId: Optional[str] = None  # URL ID for the project in the Tableau viz portal

class DropdownLoaderResponse(BaseModel):
    workbooks: List[Workbook]  # List of Workbook instances