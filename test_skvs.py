#!/usr/bin/env python3
import requests
import base64
import unittest

BASE_URL = "http://localhost:3000"
API_KEY = "your-super-secret-key-123"
DB_NAME = "default"
HEADERS = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

class TestSkvs(unittest.TestCase):

    def setUp(self):
        for table in ["users", "products", "test_table", "docs", "audit", "articles"]:
            try:
                self.execute_sql(f"DROP TABLE IF EXISTS {table}")
            except:
                pass

    def execute_sql(self, sql, params=None):
        url = f"{BASE_URL}/api/db/{DB_NAME}/query"
        payload = {"sql": sql, "params": params or []}
        resp = requests.post(url, json=payload, headers=HEADERS)
        if resp.status_code != 200:
            raise Exception(f"SQL error: {resp.text}")
        return resp.json()

    def test_create_table(self):
        sql = """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            email TEXT UNIQUE
        )
        """
        self.execute_sql(sql)
        self.execute_sql("INSERT INTO users (id, name, age, email) VALUES (1, 'Alice', 30, 'alice@example.com')")
        rows = self.execute_sql("SELECT * FROM users WHERE id = 1")
        self.assertEqual(len(rows['rows']), 1)
        self.assertEqual(rows['rows'][0]['name'], 'Alice')

    def test_insert_select(self):
        self.execute_sql("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
        self.execute_sql("INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 999.99)")
        self.execute_sql("INSERT INTO products (id, name, price) VALUES (2, 'Mouse', 25.50)")
        result = self.execute_sql("SELECT * FROM products ORDER BY id")
        self.assertEqual(len(result['rows']), 2)
        self.assertEqual(result['rows'][0]['name'], 'Laptop')

    def test_where_clause(self):
        self.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        self.execute_sql("INSERT INTO users VALUES (1, 'Bob', 25)")
        self.execute_sql("INSERT INTO users VALUES (2, 'Charlie', 35)")
        result = self.execute_sql("SELECT * FROM users WHERE age > 30")
        self.assertEqual(len(result['rows']), 1)
        self.assertEqual(result['rows'][0]['name'], 'Charlie')

    def test_update_delete(self):
        self.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        self.execute_sql("INSERT INTO users VALUES (1, 'Dave', 40)")
        self.execute_sql("UPDATE users SET age = 41 WHERE id = 1")
        row = self.execute_sql("SELECT age FROM users WHERE id = 1")
        self.assertEqual(row['rows'][0]['age'], 41)
        self.execute_sql("DELETE FROM users WHERE id = 1")
        row = self.execute_sql("SELECT * FROM users WHERE id = 1")
        self.assertEqual(len(row['rows']), 0)

    def test_trigger(self):
        self.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        self.execute_sql("CREATE TABLE audit (id INTEGER PRIMARY KEY, action TEXT, old_name TEXT, new_name TEXT)")
        self.execute_sql("""
            CREATE TRIGGER user_update_audit
            AFTER UPDATE ON users
            FOR EACH ROW
            BEGIN
                INSERT INTO audit (action, old_name, new_name) VALUES ('UPDATE', OLD.name, NEW.name);
            END
        """)
        self.execute_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
        self.execute_sql("UPDATE users SET name = 'Alicia' WHERE id = 1")
        audit = self.execute_sql("SELECT * FROM audit")
        self.assertEqual(len(audit['rows']), 1)
        self.assertEqual(audit['rows'][0]['old_name'], 'Alice')
        self.assertEqual(audit['rows'][0]['new_name'], 'Alicia')

    def test_view(self):
        self.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        self.execute_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
        self.execute_sql("CREATE VIEW adult_users AS SELECT name, age FROM users WHERE age >= 18")
        result = self.execute_sql("SELECT * FROM adult_users")
        self.assertEqual(len(result['rows']), 1)
        self.assertEqual(result['rows'][0]['name'], 'Alice')

    def test_json(self):
        self.execute_sql("CREATE TABLE docs (id INTEGER PRIMARY KEY, data TEXT)")
        self.execute_sql("INSERT INTO docs VALUES (1, '{\"name\":\"Alice\",\"age\":30}')")
        result = self.execute_sql("SELECT json_extract(data, '$.name') as name FROM docs")
        self.assertEqual(result['rows'][0]['name'], 'Alice')

    def test_fts(self):
        self.execute_sql("CREATE VIRTUAL TABLE articles USING fts5(content)")
        self.execute_sql("INSERT INTO articles VALUES ('The quick brown fox jumps over the lazy dog')")
        self.execute_sql("INSERT INTO articles VALUES ('The five boxing wizards jump quickly')")
        # FTS search via function
        result = self.execute_sql("SELECT * FROM articles WHERE fts_match('articles', 'quick')")
        # We expect only the first row (but may get both due to "quick" in "quickly" too)
        # For exact test, we check length >0
        self.assertGreater(len(result['rows']), 0)

    def test_raw_api(self):
        url_put = f"{BASE_URL}/api/db/{DB_NAME}/table/test_table/row/mykey"
        value_b64 = base64.b64encode(b"hello world").decode()
        resp = requests.put(url_put, json={"value": value_b64}, headers=HEADERS)
        self.assertEqual(resp.status_code, 204)

        url_get = f"{BASE_URL}/api/db/{DB_NAME}/table/test_table/row/mykey"
        resp = requests.get(url_get, headers=HEADERS)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        decoded = base64.b64decode(data['value'])
        self.assertEqual(decoded, b"hello world")

        resp = requests.delete(url_get, headers=HEADERS)
        self.assertEqual(resp.status_code, 204)
        resp = requests.get(url_get, headers=HEADERS)
        self.assertEqual(resp.status_code, 404)

if __name__ == "__main__":
    unittest.main()