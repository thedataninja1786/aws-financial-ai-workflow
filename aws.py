import boto3
import psycopg2
from typing import Tuple, List, Optional, Any
import pandas as pd


class S3:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name

    def __repr__(self) -> None:
        return f"S3(bucket_name={self.bucket_name})"

    def upload_string_to_s3(self, content: str, object_name: str) -> None:
        """Uploads a string as an object to an S3 bucket."""

        try:
            boto3.client("s3").put_object(
                Body=content, Bucket=self.bucket_name, Key=object_name
            )
            print(
                f"Data {object_name} were succesfully uploaded to {self.bucket_name}!"
            )
        except Exception as e:
            print(f"Failed to upload to S3: {e}")
            raise


class RedShift:
    def __init__(
        self,
        cluster: str,
        workgroup_name: str,
        region: str,
        db_name: str,
        host: str,
        port: str,
    ) -> None:
        self.cluster = cluster
        self.workgroup_name = workgroup_name
        self.region = region
        self.db_name = db_name
        self.host = host
        self.port = port
        self.user = None
        self.password = None

    def __repr__(self) -> None:
        return (
            f"RedShift(cluster={self.cluster}, workgroup_name={self.workgroup_name}, "
            f"region={self.region}, db_name={self.db_name}, host={self.host}, port={self.port})"
        )

    def generate_tmp_credentials(self) -> None:
        """Generates temporary database credentials using the AWS SDK"""

        try:
            client = boto3.client(self.cluster, region_name=self.region)
            response = client.get_credentials(
                workgroupName=self.workgroup_name,
                durationSeconds=3600,  # duration of temporary credentials
                dbName=self.db_name,
            )
            print("Temporary credentials have successfully created!")
            self.user, self.password = response["dbUser"], response["dbPassword"]

        except Exception as e:
            print(
                "The following exception has occurred while generating temporary credentials:"
            )
            print(e)
            raise

    def _connect(self) -> None:
        """Establishes connection to Redshift"""

        try:
            return psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.db_name,
                user=self.user,
                password=self.password,
            )
        except Exception as e:
            print(
                f"Failed to connect to: {self.db_name} - {self.host} on port {self.port}!"
            )
            print(e)
            raise

    def query_table(self, query: str) -> pd.DataFrame:
        try:
            with self._connect() as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            print("An error occurred during the query:", e)
            raise

    def write_data(
        self,
        table_name: str,
        data_rows: List[Tuple[Any, ...]],
        column_names: List[str],
        write_method: str,
        upsert_on: Optional[List[str]] = None,
    ) -> None:
        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                if write_method == "replace":
                    cursor.execute(f"DELETE FROM {table_name};")
                    conn.commit()
                    write_method = "append"  # proceed to append after replace

                if write_method == "append":
                    insert_query = f"""
                        INSERT INTO {table_name} ({', '.join(column_names)})
                        VALUES ({', '.join(['%s'] * len(column_names))});
                    """
                    cursor.executemany(insert_query, data_rows)
                    conn.commit()

                elif write_method == "upsert":
                    if upsert_on is None:
                        raise ValueError(
                            "upsert_on must be provided for upsert operations."
                        )

                    update_cols = [col for col in column_names if col not in upsert_on]

                    # Format SELECT source part
                    source_values = ", ".join([f"%s AS {col}" for col in column_names])

                    # Format ON condition
                    upsert_condition = " AND ".join(
                        [f"{table_name}.{col} = source.{col}" for col in upsert_on]
                    )

                    # Format UPDATE clause
                    update_clause = ", ".join(
                        [f"{col} = source.{col}" for col in update_cols]
                    )

                    # Format INSERT clause
                    insert_cols = ", ".join(column_names)
                    insert_vals = ", ".join([f"source.{col}" for col in column_names])

                    upsert_query = f"""
                    MERGE INTO {table_name}
                    USING (SELECT {source_values}) AS source
                    ON {upsert_condition}
                    WHEN MATCHED THEN UPDATE SET {update_clause}
                    WHEN NOT MATCHED THEN INSERT ({insert_cols})
                    VALUES ({insert_vals});
                """

                    cursor.executemany(upsert_query, data_rows)
                    conn.commit()

                else:
                    raise NotImplementedError(f"{write_method} is not implemented!")

                print(f"Row data successfully {write_method} on table {table_name}!")

        except Exception as e:
            print(
                f"An error occurred while {write_method} data to the table '{table_name}':",
                e,
            )
            raise

    def create_table(self, table_name: str, fields: dict) -> None:
        """Creates a table in Redshift within the specified schema."""

        try:
            columns = ", ".join([f"{col} {dtype}" for col, dtype in fields.items()])
            create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"

            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(create_query)
                conn.commit()
                print(f"Table '{table_name}' created successfully!")
        except Exception as e:
            print(f"An error occurred while creating the table '{table_name}':", e)
            raise

    def create_view() -> None: ...

    def drop_table(self, table_name: str) -> None:
        """Drops a table from the database if it exists."""

        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                drop_query = f"DROP TABLE IF EXISTS {table_name}"
                cursor.execute(drop_query)
                conn.commit()
                print("Table dropped successfuly!")
        except Exception as e:
            print("An error occurred while dropping the table:", e)
            raise
