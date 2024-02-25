import airflow
import pandas as pd
from airflow import DAG

from airflow.operators.python import PythonOperator
from datetime import timedelta
from remove_emoji import remove_emoji
import nltk
from nltk.corpus import stopwords
import stanza
from keybert import KeyBERT
from geopy.geocoders import ArcGIS
import os
from transformers import AutoModelForSequenceClassification
from transformers import BertTokenizerFast
import pickle
import torch
from sqlalchemy import create_engine
from geoalchemy2 import Geometry
import geopandas as gpd


os.environ["TORCH_EXTENSIONS_DIR"] = "/opt/airflow/dags"


default_args = {
    "owner": "Alexander Katynsus",
    # 'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    # 'retries': 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="Process_data_DAG",
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule_interval="@once",
    dagrun_timeout=timedelta(minutes=60),
    description="Process data of a city",
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
) as dag:
    emotions = {0: "NEUTRAL", 1: "POSITIVE", 2: "NEGATIVE"}

    city_name = "Новокуйбышевск"

    def download_models():
        os.environ["SENTENCE_TRANSFORMERS_HOME"] = "/opt/airflow/dags/.cache"

        try:
            stanza.download("ru")
        except:
            print("Already exists")

        nltk.download("stopwords")

        kw_model = KeyBERT()

        tokenizer = BertTokenizerFast.from_pretrained(
            "blanchefort/rubert-base-cased-sentiment", cache_dir="/opt/airflow/dags/.cache"
        )
        model = AutoModelForSequenceClassification.from_pretrained(
            "blanchefort/rubert-base-cased-sentiment", return_dict=True, cache_dir="/opt/airflow/dags/.cache"
        )

        kw_filename = "kw_model.pkl"
        #  nlp_filename = 'nlp_model.pkl'
        tokenizer_filename = "tokenizer.pkl"
        model_filename = "bert_model.pkl"

        pickle.dump(kw_model, open(f"/opt/airflow/dags/models/{kw_filename}", "wb"))
        #  pickle.dump(nlp, open(f"/opt/airflow/dags/models/{nlp_filename}", 'wb'))
        pickle.dump(tokenizer, open(f"/opt/airflow/dags/models/{tokenizer_filename}", "wb"))
        pickle.dump(model, open(f"/opt/airflow/dags/models/{model_filename}", "wb"))

    def geocode_place(place):
        from shapely import Point

        geolocator = ArcGIS(user_agent="POI")

        location = geolocator.geocode(f"{city_name} {place}", timeout=10)

        return Point(location.longitude, location.latitude)

    def create_tables_with_processed_data():
        engine = create_engine(f"postgresql://docker:docker@postgis:5432/gis")

        with engine.connect() as conn:
            conn.execute(
                """
                    CREATE TABLE IF NOT EXISTS Processed_posts (
                          name TEXT,
                          location GEOMETRY(Geometry, 4326),
                          emotion TEXT,
                          keywords TEXT)"""
            )

    def process_vk_groups_data():
        device = "cuda" if torch.cuda.is_available() else "cpu"
        nlp = stanza.Pipeline(lang="ru", processors="tokenize,ner", use_gpu=True, download_method=None, device=device)

        tokenizer = pickle.load(open("/opt/airflow/dags/models/tokenizer.pkl", "rb"))
        model = pickle.load(open("/opt/airflow/dags/models/bert_model.pkl", "rb"))

        kw_model = pickle.load(open("/opt/airflow/dags/models/kw_model.pkl", "rb"))

        engine = create_engine("postgresql://docker:docker@postgis:5432/gis")

        def analyze_groups_text(row):
            text = str(row["post"])
            print(text)

            text = remove_emoji(text)

            inputs = tokenizer(text, max_length=512, padding=True, truncation=True, return_tensors="pt")
            outputs = model(**inputs)
            predicted = torch.nn.functional.softmax(outputs.logits, dim=1)
            predicted_emotion = torch.argmax(predicted, dim=1).numpy()

            doc = nlp(text)
            entities = [[ent.text, ent.type] for sent in doc.sentences for ent in sent.ents]

            try:
                keywords = [
                    kw[0]
                    for kw in kw_model.extract_keywords(
                        text, keyphrase_ngram_range=(1, 3), stop_words=stopwords.words("russian")
                    )
                ]
            except:
                return "Undefined"

            for ent in entities:
                if ent[1] == "LOC" or ent[1] == "ORG":
                    if ent[0].lower() != city_name.lower() and ent[0].lower() != "россия":
                        location = geocode_place(ent[0])

                        tmp_gdf = gpd.GeoDataFrame({"name": [f"{ent[0]}"], "location": [location],
                                                    "emotion": [predicted_emotion[0]], "keywords": [keywords]},
                                                   geometry="location", crs=4326)
                        tmp_gdf.to_postgis(
                            "processed_posts", engine, if_exists="append", index=False,
                            dtype={"location": Geometry("POINT", srid=4326)}
                        )

                return row

            data = pd.read_sql_query("select * from posts where group_name<>'Центральный район, Санкт-Петербург'",
                                     con=engine)
            print(len(data))
            data = data.drop_duplicates(["post"])
            print(len(data))
            data.apply(analyze_groups_text, axis=1)

    def process_vk_news_data():
        device = "cuda" if torch.cuda.is_available() else "cpu"
        nlp = stanza.Pipeline(lang="ru", processors="tokenize,ner", use_gpu=True, download_method=None, device=device)

        tokenizer = pickle.load(open("/opt/airflow/dags/models/tokenizer.pkl", "rb"))
        model = pickle.load(open("/opt/airflow/dags/models/bert_model.pkl", "rb"))

        kw_model = pickle.load(open("/opt/airflow/dags/models/kw_model.pkl", "rb"))

        engine = create_engine("postgresql://docker:docker@postgis:5432/gis")

        def analyze_city_news(row):
            text = str(row["post"])
            print(text)

            text = remove_emoji(text)[:512]

            inputs = tokenizer(text, max_length=512, padding=True, truncation=True, return_tensors="pt")
            outputs = model(**inputs)
            predicted = torch.nn.functional.softmax(outputs.logits, dim=1)
            predicted_emotion = torch.argmax(predicted, dim=1).numpy()

            doc = nlp(text)
            entities = [[ent.text, ent.type] for sent in doc.sentences for ent in sent.ents]

            try:
                keywords = [
                    kw[0]
                    for kw in kw_model.extract_keywords(
                        text, keyphrase_ngram_range=(1, 3), stop_words=stopwords.words("russian")
                    )
                ]
            except:
                return "Undefined"

            for ent in entities:
                if ent[1] == "LOC" or ent[1] == "ORG":
                    if ent[0].lower() != city_name.lower() and ent[0].lower() != "россия":
                        location = geocode_place(ent[0])

                        tmp_gdf = gpd.GeoDataFrame({"name": [f"{ent[0]}"], "location": [location],
                                                    "emotion": [predicted_emotion[0]], "keywords": [keywords]},
                                                   geometry="location", crs=4326)
                        tmp_gdf.to_postgis(
                            "processed_posts", engine, if_exists="append", index=False,
                            dtype={"location": Geometry("POINT", srid=4326)}
                        )

                return row

            data = pd.read_sql_query("select * from posts where group_name<>'Центральный район, Санкт-Петербург'",
                                     con=engine)
            print(len(data))
            data = data.drop_duplicates(["post"])
            print(len(data))
            data.apply(analyze_city_news, axis=1)

    def process_yandex_data():
        device = "cuda" if torch.cuda.is_available() else "cpu"
        nlp = stanza.Pipeline(lang="ru", processors="tokenize,ner", use_gpu=True, download_method=None, device=device)

        tokenizer = pickle.load(open("/opt/airflow/dags/models/tokenizer.pkl", "rb"))
        model = pickle.load(open("/opt/airflow/dags/models/bert_model.pkl", "rb"))

        kw_model = pickle.load(open("/opt/airflow/dags/models/kw_model.pkl", "rb"))

        engine = create_engine("postgresql://docker:docker@postgis:5432/gis")

        def analyze_yandex_text(row):
            text = str(row["review"])
            location = row["location"]
            place = row["place"]

            text = remove_emoji(text)

            inputs = tokenizer(text, max_length=512, padding=True, truncation=True, return_tensors="pt")
            outputs = model(**inputs)
            predicted = torch.nn.functional.softmax(outputs.logits, dim=1)
            predicted_emotion = torch.argmax(predicted, dim=1).numpy()

            doc = nlp(text)
            entities = [[ent.text, ent.type] for sent in doc.sentences for ent in sent.ents]
            try:
                keywords = " ".join([
                    kw[0]
                    for kw in kw_model.extract_keywords(
                        text, keyphrase_ngram_range=(1, 3), stop_words=stopwords.words("russian")
                    )
                ])
            except:
                return "Undefined"
            if entities:
                for ent in entities:
                    if ent[1] == "LOC" or ent[1] == "ORG":
                        if ent[0].lower() != city_name.lower() and ent[0].lower() != "россия":
                            tmp_gdf = gpd.GeoDataFrame({"name": [f"{place}, {ent[0]}"], "location": [location],
                                                        "emotion": [predicted_emotion[0]], "keywords": [keywords]},
                                                       geometry="location", crs=4326)
                            tmp_gdf.to_postgis(
                                "processed_posts", engine, if_exists="append", index=False,
                                dtype={"location": Geometry("POINT", srid=4326)}
                            )
                    else:
                        tmp_gdf = gpd.GeoDataFrame({"name": [f"{place}"], "location": [location],
                                                    "emotion": [predicted_emotion[0]], "keywords": [keywords]},
                                                   geometry="location", crs=4326)
                        tmp_gdf.to_postgis(
                            "processed_posts", engine, if_exists="append", index=False,
                            dtype={"location": Geometry("POINT", srid=4326)}
                        )
            else:
                tmp_gdf = gpd.GeoDataFrame({"name": [place], "location": [location],
                                            "emotion": [predicted_emotion[0]], "keywords": [keywords]},
                                           geometry="location", crs=4326)
                tmp_gdf.to_postgis(
                    "processed_posts", engine, if_exists="append", index=False,
                    dtype={"location": Geometry("POINT", srid=4326)}
                )
            return row

        data = gpd.read_postgis("select * from reviews", con=engine, geom_col="location")
        data = data.drop_duplicates(["review"])
        data.apply(analyze_yandex_text, axis=1)

    def finish():
        print("FINALLY DONE!")

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables_with_processed_data,
        provide_context=True,
    )

    init_models_task = PythonOperator(
        task_id="init_models",
        python_callable=download_models,
        provide_context=True,
    )

    process_vk_groups_task = PythonOperator(
        task_id="process_vk_groups",
        python_callable=process_vk_groups_data,
        provide_context=True,
    )

    process_vk_news_task = PythonOperator(
        task_id="process_vk_news",
        python_callable=process_vk_news_data,
        provide_context=True,
    )

    process_yandex_task = PythonOperator(
        task_id="process_yandex",
        python_callable=process_yandex_data,
        provide_context=True,
    )

    finish_task = PythonOperator(
        task_id="finish",
        python_callable=finish,
        provide_context=True,
    )


(
    create_tables_task
    >> init_models_task
    >> process_vk_groups_task
    >> process_vk_news_task
    >> process_yandex_task
    >> finish_task
)
