import pandas as pd
import re
from fastapi import HTTPException
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import accuracy_score

CSV_FILE_PATH = "twitter_dataset.csv"

def preprocess_text(text):
    """Fungsi sederhana untuk membersihkan teks."""
    text = str(text).lower()  
    text = re.sub(r'[^a-zA-Z\s]', '', text)  
    text = re.sub(r'\s+', ' ', text).strip()
    return text

async def train_model_from_csv():
    """
    Melakukan preprocessing, indexing, dan training model ML
    untuk memprediksi popularitas tweet dari teksnya.
    """
    try:
        try:
            df = pd.read_csv(CSV_FILE_PATH)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"Dataset tidak ditemukan: {CSV_FILE_PATH}")
            
        df = df.dropna(subset=['Text'])

        df['cleaned_text'] = df['Text'].apply(preprocess_text)
        
        df['label'] = (df['Likes'] > 50).astype(int)

        X = df['cleaned_text']
        y = df['label']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        vectorizer = TfidfVectorizer(max_features=5000)
        X_train_tfidf = vectorizer.fit_transform(X_train)
        X_test_tfidf = vectorizer.transform(X_test)

        model = SGDClassifier(max_iter=10, random_state=42)
        model.fit(X_train_tfidf, y_train)

        y_pred = model.predict(X_test_tfidf)
        accuracy = accuracy_score(y_test, y_pred)

        return {
            "message": "Model ML berhasil dilatih",
            "model_type": "SGDClassifier (Klasifikasi Popularitas Tweet)",
            "training_epochs (max_iter)": 10,
            "dataset_total_rows": len(df),
            "training_data_rows": len(X_train),
            "test_data_rows": len(X_test),
            "model_accuracy_on_test_data": f"{accuracy * 100:.2f}%"
        }

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Gagal melatih model: {str(e)}")
