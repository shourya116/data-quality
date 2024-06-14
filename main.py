import click
from pathlib import Path
import yaml
from yaml.loader import SafeLoader
from rules import check_duplicates_in_column
from rules import check_nulls_in_column
from rules import validate_email_addresses
from logging_config import setup_logger

logger = setup_logger(__name__)

@click.command()
@click.option(
    "--config_path",
    help="Path of yaml configuration file. ",
    type=click.Path(exists=True),
    required= True,
)

def main(
    config_path: Path,
) -> None:
    
    logger.debug("Loading the configuration file... ")
    with open(config_path, 'r') as f:
       config_file = list(yaml.load_all(f, Loader=SafeLoader))

    total_checks = 0
    passed_checks = 0
    for config in config_file:
        project_id = config['project_id']
        dataset_id = config['dataset_id']
        table_name = config['table_name']
        column_name = config['column_name']
        logger.debug(f"\nApplying checks for `{project_id}.{dataset_id}.{table_name}` for column: {column_name} ")
        for rule in config['rules']:
            if 'null_check' in rule:
                total_checks += 1
                # Check for nulls
                null_count = check_nulls_in_column(project_id, dataset_id, table_name, column_name)
                if null_count == 0:
                    passed_checks += 1
                    logger.info(f"\nNull Check Passed")
                else:
                    logger.error("\nNull Check Failed")
                    logger.error(f"Number of null values in column '{column_name}': {null_count}")

            if 'uniqueness_check' in rule:
                total_checks += 1
                # check for duplicate values in a column
                duplicates = check_duplicates_in_column(project_id, dataset_id, table_name, column_name)
                if duplicates:
                    logger.error(f"\nUniqueness Check Failed")
                    logger.error(f"Duplicate values found in column '{column_name}':")
                    for value, count in duplicates:
                        logger.error(f"Value: {value}, Count: {count}")
                else:
                    passed_checks += 1
                    logger.info(f"\nUniqueness Check Passed")

            if 'regex_valid_email' in rule:
                total_checks += 1
                # Validate email addresses
                invalid_emails = validate_email_addresses(project_id, dataset_id, table_name, column_name)

                if invalid_emails:
                    logger.error("\nRegex check Failed")
                    logger.error(f"Invalid email addresses found in column '{column_name}':")
                    for email in invalid_emails:
                        logger.error(email)
                else:
                    passed_checks += 1
                    logger.info(f"\nRegex check Passed")

    if total_checks != passed_checks:
        raise Exception(f"\nTotal Checks: {total_checks} Passed Checks: {passed_checks}")
    else:
        logger.info(f"\nAll {total_checks} Checks Passed.")

if __name__ == "__main__":
    main()