from google.cloud import bigquery

def check_duplicates_in_column(project_id, dataset_id, table_id, column_name) -> list:
    # initialize Bigquery Client
    client = bigquery.Client(project=project_id)

    # query to count duplicates in the specified column
    query = f"""
        SELECT
            {column_name},
            COUNT(*) as count
        FROM
            `{project_id}.{dataset_id}.{table_id}`
        GROUP BY
            {column_name}
        HAVING
            count > 1
        ORDER BY
            count DESC
    """

    # execute the query 
    query_job = client.query(query)

    results = query_job.result()

    # Check if there are duplicates
    duplicates = []
    for row in results:
        duplicates.append((row[column_name], row['count']))

    return duplicates

def main():

    duplicates = check_duplicates_in_column("<project-id>", "<dataset-id>", "<table-name>", "<column-name>")
    
    if duplicates:
        print(f"Duplicate values found in column '{column_name}':")
        for value, count in duplicates:
            print(f"Value: {value}, Count: {count}")
    else:
        print(f"No duplicate values found in column '{column_name}'.")

if __name__ == "__main__":
    main()