from google.cloud import bigquery
import re

def check_duplicates_in_column(project_id, dataset_id, table_id, column_name):
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

    # fetch the result
    results = query_job.result()

    # Check if there are duplicates
    duplicates = []
    for row in results:
        duplicates.append((row[column_name], row['count']))

    return duplicates

def check_nulls_in_column(project_id, dataset_id, table_id, column_name):
    # initialize Bigquery Client
    client = bigquery.Client(project=project_id)

    # query to check for null values
    query = f"""
        SELECT
            COUNT(*) as null_count
        FROM
            `{project_id}.{dataset_id}.{table_id}`
        WHERE
            {column_name} IS NULL
    """

    # execute the query 
    query_job = client.query(query)

    # fetch the result
    results = query_job.result()

    # Get the null count
    null_count = next(results).null_count

    return null_count

def validate_email_addresses(project_id, dataset_id, table_name, column_name):
    # initialize Bigquery Client
    client = bigquery.Client(project=project_id)

    query = f"""
        SELECT
            {column_name}
        FROM
            `{project_id}.{dataset_id}.{table_name}`
        WHERE
            {column_name} IS NOT NULL
    """

    # Execute the query
    query_job = client.query(query)

    # Fetch the result
    results = query_job.result()

    # Email validation regex pattern
    email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

    # Check for invalid email addresses
    invalid_emails = []
    for row in results:
        email = row[column_name]
        if not email_pattern.match(email):
            invalid_emails.append(email)

    return invalid_emails