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
        # Get column information from the source table
        source_cursor.execute(f"PRAGMA table_info({table})")
        columns = source_cursor.fetchall()

        # Prepare CREATE TABLE statement
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table} ("
        create_table_sql += ", ".join([f"{col[1]} {col[2]}" for col in columns])
        create_table_sql += ")"

        # Create the table in the target database
        target_cursor.execute(create_table_sql)

        # Get column names
        column_names = ", ".join([col[1] for col in columns])

        # Get all data from the source table
        source_cursor.execute(f"SELECT * FROM {table}")
        rows = source_cursor.fetchall()

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
