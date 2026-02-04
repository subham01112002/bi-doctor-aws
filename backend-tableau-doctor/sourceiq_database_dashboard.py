# Get datasource LUID from the output above
datasource_luid = "5aec29ac-d2ef-4cfc-a8bc-96bc17729e9a"

# VizQL Data Service query
query_url = f"{server}/api/v1/vizql-data-service/query-datasource"
vds_headers = {
    "Content-Type": "application/json",
    "X-Tableau-Auth": token
}

query_body = {
    "datasource": {
        "datasourceLuid": datasource_luid
    },
    "query": {
    "fields": [
      { "fieldCaption": "Event Id" },
      { "fieldCaption": "Event Date" },
      { "fieldCaption": "Event Type" },
      { "fieldCaption": "Event Name" },
      { "fieldCaption": "Item Type" },
      { "fieldCaption": "Item Name" },
      { "fieldCaption": "Item LUID" },
      { "fieldCaption": "Actor User Id" },
      { "fieldCaption": "Actor User Name" },
      { "fieldCaption": "Actor Site Role" },
      { "fieldCaption": "Admin Insights Published At" }
    ],"filters": [
      {
        "field": {
          "fieldCaption": "Event Name"
        },
        "filterType": "SET",
        "values": ["Delete View", "Publish View", "Access View", "Delete Workbook", "Publish Workbook", "Access Data Source", "Publish Data Source", "Delete Data Source", "Download Workbook", "Download Data Source", "Update Workbook"],
        "exclude": False
      }
    ]
    },
    "options": {
        "debug": True
    }
}




query_response = requests.post(query_url, headers=vds_headers, json=query_body)
print(query_response)
results = query_response.json()
print(results)
# Convert to DataFrame
import pandas as pd
df = pd.DataFrame(results.get('data', []))

column_mapping = {
    'Event Id': 'event_id',
    'Event Date': 'event_date',
    'Event Type': 'event_type',
    'Event Name': 'event_name',
    'Item Type': 'item_type',
    'Item Name': 'item_name',
    'Item LUID': 'item_luid',
    'Actor User Id': 'user_id',
    'Actor User Name': 'user_name',
    'Actor Site Role': 'user_role',
    'Admin Insights Published At': 'sync_date'
}
df = df.rename(columns=column_mapping)

print(df)
# Save DataFrame to CSV in Colab
df.to_csv('data.csv', index=False)
print("DataFrame saved to data.csv")

