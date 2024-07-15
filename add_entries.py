import sqlite3
import click

@click.command()
@click.argument('source_db', type=click.Path(exists=True))
@click.argument('target_db', type=click.Path())
@click.option('--table', default='miner_stats', help='Name of the table to copy')
def copy_table_data(source_db, target_db, table):
    """
    Copy data from SOURCE_DB to TARGET_DB using INSERT OR IGNORE.
    
    SOURCE_DB: Path to the source SQLite database file.
    TARGET_DB: Path to the target SQLite database file (will be created if it doesn't exist).
    """
    # Connect to the source database
    source_conn = sqlite3.connect(source_db)
    source_cursor = source_conn.cursor()

    # Connect to the target database
    target_conn = sqlite3.connect(target_db)
    target_cursor = target_conn.cursor()

    try:
        # Get the table structure from the source database
        source_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
        table_schema = source_cursor.fetchone()[0]

        # Create the table in the target database if it doesn't exist
        target_cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM ({table_schema}) WHERE 0")

        # Get all data from the source table
        source_cursor.execute(f"SELECT * FROM {table}")
        rows = source_cursor.fetchall()

        # Get column names
        source_cursor.execute(f"PRAGMA table_info({table})")
        columns = [column[1] for column in source_cursor.fetchall()]
        column_names = ", ".join(columns)

        # Insert all rows into the target table using INSERT OR IGNORE
        target_cursor.executemany(
            f"INSERT OR IGNORE INTO {table} ({column_names}) VALUES ({','.join(['?' for _ in columns])})",
            rows
        )

        # Commit the changes
        target_conn.commit()

        click.echo(f"Successfully copied {target_cursor.rowcount} new rows from {source_db} to {target_db}")

    except sqlite3.Error as e:
        click.echo(f"An error occurred: {e}", err=True)

    finally:
        # Close the connections
        source_conn.close()
        target_conn.close()

if __name__ == '__main__':
    copy_table_data()
