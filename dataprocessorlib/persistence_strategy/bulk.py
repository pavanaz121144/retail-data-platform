class BulkPersistenceStrategy:
    def __init__(self, session):
        self.session = session

    def persist(self, df, table: str, mode: str = "append"):
        df.write.save_as_table(table, mode=mode, column_order="name")

    def insert(self, df, table: str):
        """INSERT INTO ... SELECT: plain DML (no CREATE TABLE IF NOT EXISTS), so it can run inside a
        transaction. `df` should be materialised (cache_result) so its SQL is a single SELECT."""
        select_sql = df.queries["queries"][-1]
        columns = ", ".join(df.columns)
        self.session.sql(f"INSERT INTO {table} ({columns}) {select_sql}").collect()
