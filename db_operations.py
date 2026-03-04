import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor, execute_batch
from my_utilities import get_configs


class DatabaseOperations:

    def __init__(self):
        config = get_configs("database_config")
        config["dbname"] = "postgres"

        self.connection = psycopg2.connect(**config)
        self.connection.autocommit = True
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)

    # -----------------------------------------------------
    # DATABASE
    # -----------------------------------------------------

    def create_database(self, db_name: str) -> bool:
        try:
            self.cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (db_name,)
            )

            if not self.cursor.fetchone():
                self.cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(db_name)
                    )
                )
                print(f"Database '{db_name}' created.")
            else:
                print(f"Database '{db_name}' already exists.")

            return True

        except Exception as e:
            print(f"Could not create database {db_name}: {e}")
            return False

    def connect_to_database(self, db_name: str):
        self.cursor.close()
        self.connection.close()

        config = get_configs("database_config")
        config["dbname"] = db_name

        self.connection = psycopg2.connect(**config)
        self.connection.autocommit = False
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)

    # -----------------------------------------------------
    # TABLE
    # -----------------------------------------------------

    def create_table(self, tb_name: str) -> bool:
        try:
            query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    url TEXT NOT NULL UNIQUE,
                    title TEXT,
                    tender_organisation TEXT,
                    place_of_performance TEXT,
                    type_of_procedure TEXT,
                    publication_date TEXT,
                    request_deadline TEXT,
                    tender_deadline TEXT,
                    tender_valid_until TEXT,
                    question_deadline TEXT,
                    description TEXT,
                    additional_cpv TEXT
                );
            """).format(sql.Identifier(tb_name))

            self.cursor.execute(query)
            self.connection.commit()
            return True

        except Exception as e:
            print(f"Error while creating table: {e}")
            self.connection.rollback()
            return False

    # -----------------------------------------------------
    # CUSTOM TABLE
    # -----------------------------------------------------

    def create_custom_table(self, tb_name: str, fields: list[tuple[str, str]]) -> bool:
        try:
            # Build column definitions safely
            columns = [
                sql.SQL("{} {}").format(
                    sql.Identifier(field_name),
                    sql.SQL(field_type)
                )
                for field_name, field_type in fields
            ]

            query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    {}
                );
            """).format(
                sql.Identifier(tb_name),
                sql.SQL(", ").join(columns)
            )

            self.cursor.execute(query)
            self.connection.commit()
            return True

        except Exception as e:
            print(f"Error while creating table: {e}")
            self.connection.rollback()
            return False

    # -----------------------------------------------------
    # INSERT ONE
    # -----------------------------------------------------

    def insert_single_data(self, table: str, row: dict) -> bool:

        cleaned_row = {
            k: (None if v == "-" else v)
            for k, v in row.items()
        }

        columns = cleaned_row.keys()
        values = list(cleaned_row.values())

        query = sql.SQL("""
            INSERT INTO {table} ({fields})
            VALUES ({placeholders})
            ON CONFLICT (url) DO NOTHING
        """).format(
            table=sql.Identifier(table),
            fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(', ').join(
                sql.Placeholder() * len(columns)
            )
        )

        try:
            self.cursor.execute(query, values)
            self.connection.commit()
            return True

        except Exception as e:
            print(f"Error while inserting data: {e}")
            self.connection.rollback()
            return False

    # -----------------------------------------------------
    # INSERT MANY (FAST VERSION)
    # -----------------------------------------------------

    def insert_many_data(self, table: str, rows: list[dict]) -> bool:

        if not rows:
            return True

        try:
            cleaned_rows = []
            for row in rows:
                cleaned_rows.append({
                    k: (None if v == "-" else v)
                    for k, v in row.items()
                })

            columns = cleaned_rows[0].keys()

            query = sql.SQL("""
                INSERT INTO {table} ({fields})
                VALUES ({placeholders})
                ON CONFLICT (url) DO NOTHING
            """).format(
                table=sql.Identifier(table),
                fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                placeholders=sql.SQL(', ').join(
                    sql.Placeholder() * len(columns)
                )
            )

            values = [tuple(row[col] for col in columns) for row in cleaned_rows]

            execute_batch(self.cursor, query, values)
            self.connection.commit()

            return True

        except Exception as e:
            print(f"Error while inserting many: {e}")
            self.connection.rollback()
            return False

    # -----------------------------------------------------
    # SELECT ONE
    # -----------------------------------------------------

    def select_single_data(self, table: str) -> dict | None:

        query = sql.SQL("SELECT * FROM {} LIMIT 1").format(
            sql.Identifier(table)
        )

        try:
            self.cursor.execute(query)
            return self.cursor.fetchone()

        except Exception as e:
            print(f"Error while selecting data: {e}")
            return None

    # -----------------------------------------------------
    # SELECT ALL
    # -----------------------------------------------------

    def select_all_data(self, table: str) -> list[dict] | None:

        query = sql.SQL("SELECT * FROM {}").format(
            sql.Identifier(table)
        )

        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()

        except Exception as e:
            print(f"Error while selecting data: {e}")
            return None

    # -----------------------------------------------------
    # CLOSE
    # -----------------------------------------------------

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

