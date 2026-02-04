# Tableau Migration Tool

A Python tool for managing Tableau Server datasources and workbooks across multiple environments (dev, test, prod).

## Features

- ✅ List datasources, workbooks, and projects
- ✅ Download datasources and workbooks
- ✅ Publish datasources and workbooks to different projects
- ✅ Update datasource database connections
- ✅ Update workbook datasource references
- ✅ Support for multiple environments (dev, test, prod)
- ✅ Secure credential management via environment variables

## Project Structure

```
tableau-migration/
├── main.py                  # Main entry point
├── config.py                # Configuration management
├── tableau_client.py        # Base Tableau API client
├── datasource_manager.py    # Datasource operations
├── workbook_manager.py      # Workbook operations
├── connection_manager.py    # Database connection operations
├── requirements.txt         # Python dependencies
├── .env.example            # Environment variables template
├── .gitignore              # Git ignore rules
└── README.md               # This file
```

## Setup

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd tableau-migration
```

### 2. Create virtual environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env` with your actual credentials:

```env
DEV_TABLEAU_SERVER=https://your-server.com
DEV_TABLEAU_PAT_NAME=your-pat-name
DEV_TABLEAU_PAT_SECRET=your-pat-secret
# ... etc
```

### 5. Load environment variables

```bash
# Install python-dotenv if not already installed
pip install python-dotenv

# In your Python script or shell
from dotenv import load_dotenv
load_dotenv()
```

Or use a `.env` loader in your terminal.

## Usage

### List Resources

```bash
# List all datasources in dev environment
python main.py --env dev list datasources

# List all workbooks
python main.py --env dev list workbooks

# List all projects
python main.py --env dev list projects
```

### Migrate Datasource

```bash
# Download from dev and publish to prod
python main.py --env dev migrate-datasource \
    --id "datasource-uuid-here" \
    --project-id "target-project-uuid"
```

### Migrate Workbook

```bash
# Basic workbook migration
python main.py --env dev migrate-workbook \
    --id "workbook-uuid-here" \
    --project-id "target-project-uuid"

# With datasource reference updates
python main.py --env dev migrate-workbook \
    --id "workbook-uuid-here" \
    --project-id "target-project-uuid" \
    --ds-mapping datasource_mapping.json
```

Example `datasource_mapping.json`:
```json
{
    "DS_S1_PP_17658825290080": "DS_S1_PP_17659019971400",
    "old_content_url": "new_content_url"
}
```

### Update Database Connection

```bash
python main.py --env prod update-connection \
    --id "datasource-uuid" \
    --host "mysql-host.example.com" \
    --port "3306" \
    --username "dbuser" \
    --password "dbpassword"
```

## Common Workflows

### Workflow 1: Promote Datasource from Dev to Prod

```bash
# 1. List datasources in dev to get the ID
python main.py --env dev list datasources

# 2. List projects in prod to get target project ID
python main.py --env prod list projects

# 3. Download and publish to prod
python main.py --env dev migrate-datasource \
    --id "YOUR_DS_ID" \
    --project-id "PROD_PROJECT_ID"

# 4. Update the connection to prod database
python main.py --env prod update-connection \
    --id "NEW_DS_ID" \
    --host "prod-db-host" \
    --port "3306" \
    --username "prod_user" \
    --password "prod_pass"
```

### Workflow 2: Promote Workbook with Updated Datasource References

```bash
# 1. Get the new datasource content URL
python main.py --env prod list datasources
# Note the contentUrl for the datasource

# 2. Create datasource mapping file
cat > datasource_mapping.json << EOF
{
    "old_dev_content_url": "new_prod_content_url"
}
EOF

# 3. Migrate workbook with updated references
python main.py --env dev migrate-workbook \
    --id "WORKBOOK_ID" \
    --project-id "PROD_PROJECT_ID" \
    --ds-mapping datasource_mapping.json
```

## Advanced Usage

### Using as a Python Library

```python
from config import Config
from tableau_client import TableauClient
from datasource_manager import DatasourceManager
from workbook_manager import WorkbookManager
from connection_manager import ConnectionManager

# Initialize
config = Config.from_env('prod')
client = TableauClient(config)

# Use managers
ds_manager = DatasourceManager(client)
wb_manager = WorkbookManager(client)
conn_manager = ConnectionManager(client)

# Example: List all datasources
datasources = ds_manager.list_datasources()
for ds in datasources:
    print(f"{ds['name']}: {ds['id']}")

# Clean up
client.sign_out()
```

### Context Manager Usage

```python
from config import Config
from tableau_client import TableauClient

config = Config.from_env('dev')

with TableauClient(config) as client:
    # Your code here
    # Client automatically signs out when done
    pass
```

## Security Best Practices

1. **Never commit credentials**: The `.gitignore` file excludes `.env` files
2. **Use Personal Access Tokens**: More secure than username/password
3. **Rotate tokens regularly**: Update your PATs periodically
4. **Use separate tokens per environment**: Don't reuse tokens across dev/test/prod
5. **Limit token permissions**: Create tokens with minimum required permissions

## Troubleshooting

### Authentication Failed

- Check your PAT name and secret in `.env`
- Verify the site content URL is correct
- Ensure your token hasn't expired

### Connection Update Failed

- Verify the datasource ID is correct
- Check database credentials
- Ensure the database host is accessible from Tableau Server

### Workbook References Not Updated

- Verify the datasource mapping file format
- Check that content URLs are exact matches
- Ensure the workbook file is a `.twb` (not `.twbx`)

## Contributing

1. Create a feature branch
2. Make your changes
3. Test thoroughly
4. Submit a pull request

## License

[Your License Here]

## Support

For issues and questions, please open a GitHub issue.
