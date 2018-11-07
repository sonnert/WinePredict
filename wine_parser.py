import pandas as pd
import numpy as np
import nltk

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

from dbpkg.psql_connector import PSQLConnector

class WineParser:
    def __init__(self, path=""):
        self.path = path

    def extract(self):
        df = pd.read_csv(self.path)

        return df[['variety', 'country', 'description']]

    def parse(self, df):
        """
        Parses the winemag data reviews, removing rows containing blends and removing target words from reviews.
        """
        remove_list = ['Blend', 'blend', '-']
        print("Cleaning varieties of blends...")
        df = df[~df['variety'].str.contains('|'.join(remove_list))].reset_index()

        descriptions = df['description']
        varieties = df['variety'].unique().tolist()
        print("Number of varieties after cleaning:", len(varieties))

        descriptions_cleaned = []
        print("Cleaning reviews of varieties...")
        for doc in descriptions:
            for term in varieties:
                if term in doc:
                    doc = doc.replace(term, "")
            descriptions_cleaned.append(doc)

        return pd.DataFrame({'review': descriptions_cleaned, 'variety': df['variety']})

    def train(self, data):
        """
        Trains a simple random forest classifier and prints the accuracy of the resulting model.
        """
        corpus = data['review']
        vectorizer = CountVectorizer(stop_words='english')
        clf = RandomForestClassifier(n_estimators=100, n_jobs=-1, verbose=1)

        X = vectorizer.fit_transform(corpus)
        y = data['variety']

        X_tr, X_te, y_tr, y_te = train_test_split(X, y, train_size=0.85)

        print("Training...")
        fit = clf.fit(X_tr, y_tr)

        print("Test accuracy:", fit.score(X_te, y_te))

if __name__ == "__main__":
    wp = WineParser("_winemag_data_first150k.csv")
    #df = wp.extract()
    #data = wp.parse(df)

    psql = PSQLConnector()
    df = psql.get_entries()

    wp.train(df)