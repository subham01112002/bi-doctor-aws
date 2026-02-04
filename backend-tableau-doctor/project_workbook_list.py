# from http import server
import tableauserverclient as TSC
import yaml
from typing import List, Dict


class TableauCloudClient:
    """
    Tableau Cloud - Unified Client
    1. List all projects with ID and name
    2. Get workbooks with ONLY sqlproxy datasources in a specific project
    """

    def __init__(self, token_name: str, token_value: str, config_path: str):
        self.token_name = token_name
        self.token_value = token_value
        self.config = self._load_config(config_path)

        self.server_url = self.config["tableau"]["server"]["url"]
        self.site_id = self.config["tableau"]["site"]["id"]

        self.server = TSC.Server(self.server_url, use_server_version=True)
        self.auth = TSC.PersonalAccessTokenAuth(
            token_name=self.token_name,
            personal_access_token=self.token_value,
            site_id=self.site_id
        )

    @staticmethod
    def _load_config(config_path: str) -> Dict:
        with open(config_path, "r") as file:
            return yaml.safe_load(file)

    def list_all_projects(self) -> List[TSC.ProjectItem]:
        """
        Connect to Tableau Cloud and list all projects with IDs and names
        """

        try:
            with self.server.auth.sign_in(self.auth):
                print(f"✓ Successfully connected to: {self.server_url}")
                print(f"✓ Site: {self.site_id if self.site_id else 'Default'}")
                print("=" * 70)

                all_projects, pagination = self.server.projects.get()

                print(f"\n Total Projects Found: {pagination.total_available}\n")
                print(f"{'Project ID':<40} {'Project Name'}")
                print("=" * 70)

                for project in all_projects:
                    print(f"{project.id:<40} {project.name}")

                print("\n" + "=" * 70)
                print(f"✓ Listed {len(all_projects)} projects successfully!\n")

                return all_projects

        except Exception as e:
            print(f"✗ Error in list_all_projects: {str(e)}")
            raise

    def get_workbooks_with_sqlproxy_only(
        self,
        target_project_id: str
    ) -> List[Dict]:
        """
        Get workbooks in a project that use ONLY sqlproxy datasources
        """
        try:
            with self.server.auth.sign_in(self.auth):
                print(f"✓ Successfully connected to: {self.server_url}")
                print(f"✓ Site: {self.site_id}")
                print(f"✓ Target Project ID: {target_project_id}")
                print("=" * 70)
                
                all_published_datasources, _ = self.server.datasources.get()
                # print(f"\n Total Published Datasources Found: {all_published_datasources}\n")
                
                # Handle duplicate datasource names by storing all datasources with same name
                published_ds_registry = {}
                for ds in all_published_datasources:
                    if ds.name not in published_ds_registry:
                        published_ds_registry[ds.name] = []
                    published_ds_registry[ds.name].append(ds)
                # published_ds_registry = {ds.name: ds for ds in all_published_datasources}
                
                all_workbooks, _ = self.server.workbooks.get()
                workbooks_in_project = [wb for wb in all_workbooks if wb.project_id == target_project_id]
                
                qualified_workbooks = []
                
                for wb in workbooks_in_project:
                    self.server.workbooks.populate_connections(wb)
                    
                    if not wb.connections:
                        continue
                    
                    all_published = True
                    all_sqlproxy = True
                    datasources = []
                    
                    for conn in wb.connections:
                        if not conn.connection_type or conn.connection_type.lower() != 'sqlproxy':
                            all_sqlproxy = False
                            break
                        
                        # Get all datasources with this name
                        matching_datasources = published_ds_registry.get(conn.datasource_name, [])

                        # If multiple datasources have same name, try to match by checking if datasource is in same project as workbook
                        published_ds = None
                        if len(matching_datasources) == 1:
                            published_ds = matching_datasources[0]
                        elif len(matching_datasources) > 1:
                            # Try to find datasource in same project as workbook
                            for ds in matching_datasources:
                                if ds.project_id == target_project_id:
                                    published_ds = ds
                                    break
                            # If not found in same project, use first match (you may want to log a warning here)
                            if not published_ds:
                                published_ds = matching_datasources[0]
                                print(f"Warning: Multiple datasources named '{conn.datasource_name}' found. Using first match.")
                        
                        # published_ds = published_ds_registry.get(conn.datasource_name)
                        if not published_ds:
                            all_published = False
                            # print(f"Warning: Datasource '{conn.datasource_name}' not found in published datasources")
                            break
                        
                        datasources.append({
                            'name': conn.datasource_name,
                            'luid': published_ds.id,
                            'connection_type': conn.connection_type,
                            'project_name': published_ds.project_name if hasattr(published_ds, 'project_name') else None,
                        })
                    
                    if all_published and all_sqlproxy:
                        qualified_workbooks.append({
                            'luid': wb.id,
                            'name': wb.name,
                            'total_datasources': len(datasources),
                            'datasources': datasources
                        })
                
                
                if qualified_workbooks:
                    for wb_info in qualified_workbooks:
                        print(f"Workbook: {wb_info['name']}")
                        print(f"   Workbook ID: {wb_info['luid']}")
                        print(f"   Total Published Sqlproxy Datasources: {wb_info['total_datasources']}")
                        
                        for idx, ds in enumerate(wb_info['datasources'], 1):
                            print(f"   [{idx}] Datasource: {ds['name']}")
                            print(f"       REST API ID: {ds['luid']}")
                            print(f"       Connection Type: {ds['connection_type']}")
                            if ds['project_name']:
                                print(f"       Datasource Project: {ds['project_name']}")
                        
                        print("-" * 100)
                
                return qualified_workbooks
                
        except Exception as e:
            print(f"Error: {str(e)}")
            import traceback
            traceback.print_exc()
            raise