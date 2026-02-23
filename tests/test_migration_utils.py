import unittest
from datetime import datetime, timedelta
import types

import migration_utils as mu


class DummyCxOracle:
    class Error(Exception):
        pass

    DB_TYPE_CLOB = object()
    DB_TYPE_BLOB = object()
    DB_TYPE_LONG = object()
    DB_TYPE_LONG_RAW = object()
    TIMESTAMP = object()


class DummyBatchError:
    def __init__(self, message):
        self.message = message


class FakeTargetCursor:
    def __init__(self, batch_error_count=0):
        self._batch_error_count = batch_error_count
        self._batch_errors = []

    def executemany(self, sql, rows, batcherrors=False):
        if batcherrors and self._batch_error_count > 0:
            self._batch_errors = [DummyBatchError("dup key")] * self._batch_error_count
        else:
            self._batch_errors = []

    def getbatcherrors(self):
        return self._batch_errors

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeTargetConn:
    username = "TEST"

    def __init__(self, batch_error_count=0):
        self._batch_error_count = batch_error_count

    def cursor(self):
        return FakeTargetCursor(batch_error_count=self._batch_error_count)

    def commit(self):
        pass

    def rollback(self):
        pass


class FakeSourceCursor:
    def __init__(self, rows, columns):
        self._rows = rows
        self._columns = columns
        self.description = [(c,) for c in columns]
        self._idx = 0
        self.arraysize = 0

    def execute(self, query, params):
        self._idx = 0

    def fetchmany(self, size):
        if self._idx >= len(self._rows):
            return []
        chunk = self._rows[self._idx : self._idx + size]
        self._idx += size
        return chunk

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeSourceConn:
    def __init__(self, rows, columns):
        self._rows = rows
        self._columns = columns

    def cursor(self):
        return FakeSourceCursor(self._rows, self._columns)


class MigrationUtilsTests(unittest.TestCase):
    def test_insert_mode_returns_error_count(self):
        # Arrange: force cx_Oracle presence and simulate batch errors
        old_cx = mu.cx_Oracle
        mu.cx_Oracle = DummyCxOracle

        try:
            rows = [(1, "a"), (2, "b"), (3, "c")]
            columns = ["ID", "VAL"]
            conn = FakeTargetConn(batch_error_count=1)

            # Act
            success, errors = mu._load_data_merge(conn, "TB", [], columns, rows)

            # Assert
            self.assertEqual(success, 2)
            self.assertEqual(errors, 1)
        finally:
            mu.cx_Oracle = old_cx

    def test_migrate_returns_max_ts_from_data(self):
        # Arrange: avoid DB dependencies
        old_cx = mu.cx_Oracle
        old_get_table_columns = mu.get_table_columns
        old_loader = mu._load_data_merge

        mu.cx_Oracle = None
        mu.get_table_columns = lambda *_: []

        def _fake_loader(*_args, **_kwargs):
            rows = _args[4]
            return len(rows), 0

        mu._load_data_merge = _fake_loader

        start = datetime(2026, 2, 1, 0, 0, 0)
        end = datetime(2026, 2, 1, 2, 0, 0)
        rows = [
            (1, datetime(2026, 2, 1, 0, 30, 0)),
            (2, datetime(2026, 2, 1, 1, 30, 0)),
        ]
        columns = ["ID", "EVENT_TIME"]
        source_conn = FakeSourceConn(rows, columns)
        target_conn = object()

        # Act
        result = mu.migrate(
            table_name="TB",
            p_keys=["ID"],
            date_column_name="EVENT_TIME",
            fetchsize=100,
            start_date=start,
            end_date=end,
            source_conn=source_conn,
            target_conn=target_conn,
        )

        # Assert: expect max_ts to be the max timestamp from rows, not end_date
        self.assertEqual(result["max_ts"], datetime(2026, 2, 1, 1, 30, 0))

        # Cleanup
        mu.cx_Oracle = old_cx
        mu.get_table_columns = old_get_table_columns
        mu._load_data_merge = old_loader


if __name__ == "__main__":
    unittest.main()
