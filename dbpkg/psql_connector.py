import psycopg2
import pandas as pd

class PSQLConnector:
    def __init__(self):
        self.conn = psycopg2.connect("dbname='wine_reviews' user='adso' host='localhost'")
        self.cur = self.conn.cursor()

    def list_tables(self):
        self.cur.execute("""SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema' """)
        for e in self.cur.fetchall():
            print(e)

    def create_table(self):
        self.cur.execute("""CREATE TABLE reviews(
            id serial,
            review varchar(10000),
            variety varchar(100),
            primary key(id)
            )""")
        self.conn.commit()

    def drop_table(self):
        self.cur.execute("""DROP TABLE reviews""")
        self.conn.commit()

    def insert_entries(self, df):
        values = df.values.tolist()
        self.cur.executemany("""INSERT INTO reviews(review, variety) VALUES (%s, %s)""", values)
        self.conn.commit()

    def get_entries(self):
        self.cur.execute("""SELECT * FROM reviews""")
        data = self.cur.fetchall()
        df = pd.DataFrame(data)
        df = df.iloc[:,1:]
        df.columns = ['review', 'variety']

        return df

if __name__ == "__main__":
    psc = PSQLConnector()
    psc.list_tables()