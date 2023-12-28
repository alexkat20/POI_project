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

        tokenizer = BertTokenizerFast.from_pretrained("blanchefort/rubert-base-cased-sentiment",
                                                      cache_dir="/opt/airflow/dags/.cache")
        model = AutoModelForSequenceClassification.from_pretrained(
            "blanchefort/rubert-base-cased-sentiment", return_dict=True,
            cache_dir="/opt/airflow/dags/.cache"
        )

        kw_filename = 'kw_model.pkl'
        #  nlp_filename = 'nlp_model.pkl'
        tokenizer_filename = 'tokenizer.pkl'
        model_filename = 'bert_model.pkl'

        pickle.dump(kw_model, open(f"/opt/airflow/dags/models/{kw_filename}", 'wb'))
        #  pickle.dump(nlp, open(f"/opt/airflow/dags/models/{nlp_filename}", 'wb'))
        pickle.dump(tokenizer, open(f"/opt/airflow/dags/models/{tokenizer_filename}", 'wb'))
        pickle.dump(model, open(f"/opt/airflow/dags/models/{model_filename}", 'wb'))


    def deprecated_get_keywords(text):
        kw_model = pickle.load(open("/opt/airflow/dags/models/kw_model.pkl", 'rb'))
        try:
            return kw_model.extract_keywords(text, keyphrase_ngram_range=(1, 3), stop_words=stopwords.words("russian"))
        except:
            return "Undefined"

    def deprecated_stanza_nlp_ru(text):
        doc = nlp(text)
        return [[ent.text, ent.type] for sent in doc.sentences for ent in sent.ents]

    def deprecated_predict_emotion(text):

        tokenizer = pickle.load(open("/opt/airflow/dags/models/tokenizer.pkl", 'rb'))
        model = pickle.load(open("/opt/airflow/dags/models/bert_model.pkl", 'rb'))

        inputs = tokenizer(text, max_length=512, padding=True, truncation=True, return_tensors="pt")
        outputs = model(**inputs)
        predicted = torch.nn.functional.softmax(outputs.logits, dim=1)
        predicted = torch.argmax(predicted, dim=1).numpy()
        return predicted

    def geocode_place(place):
        geolocator = ArcGIS(user_agent="POI")

        location = geolocator.geocode(f"{city_name} {place}", timeout=10)

        return location

    def deprecated_analyze_text(row):
        text = row["post"]
        print(text)

        places = {"places_names": [], "places": [], "emotions": [], "keywords": []}

        text = remove_emoji(text)

        predicted_emotion = predict_emotion(text)

        entities = stanza_nlp_ru(text)

        keywords = get_keywords(text)

        for ent in entities:
            if ent[1] == "LOC" or ent[1] == "ORG":
                if ent[0].lower() != city_name.lower() and ent[0].lower() != "россия":
                    location = geocode_place(ent[0])
                    places["places_names"].append(ent[0])
                    places["places"].append(location)
                    places["emotions"].append(emotions[predicted_emotion[0]])
                    places["keywords"].append(keywords)

        return pd.Series(places)

    def deprecated_analyze_yandex_text(row):
        text = row["review"]

        places = {"emotions": [], "keywords": []}

        text = remove_emoji(text)

        predicted_emotion = predict_emotion(text)

        keywords = get_keywords(text)

        places["emotions"].append(emotions[predicted_emotion[0]])
        places["keywords"].append(keywords)

        return pd.Series(places)

    def process_vk_groups_data():

        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        nlp = stanza.Pipeline(lang="ru", processors="tokenize,ner", use_gpu=True, download_method=None, device=device)

        tokenizer = pickle.load(open("/opt/airflow/dags/models/tokenizer.pkl", 'rb'))
        model = pickle.load(open("/opt/airflow/dags/models/bert_model.pkl", 'rb'))

        kw_model = pickle.load(open("/opt/airflow/dags/models/kw_model.pkl", 'rb'))

        def analyze_groups_text(row):
            text = str(row["post"])
            print(text)

            places = {"places_names": [], "places": [], "emotions": [], "keywords": []}

            text = remove_emoji(text)

            inputs = tokenizer(text, max_length=512, padding=True, truncation=True, return_tensors="pt")
            outputs = model(**inputs)
            predicted = torch.nn.functional.softmax(outputs.logits, dim=1)
            predicted_emotion = torch.argmax(predicted, dim=1).numpy()

            doc = nlp(text)
            entities = [[ent.text, ent.type] for sent in doc.sentences for ent in sent.ents]

            try:
                keywords = kw_model.extract_keywords(text, keyphrase_ngram_range=(1, 3),
                                                 stop_words=stopwords.words("russian"))
            except:
                return "Undefined"

            for ent in entities:
                if ent[1] == "LOC" or ent[1] == "ORG":
                    if ent[0].lower() != city_name.lower() and ent[0].lower() != "россия":
                        location = geocode_place(ent[0])
                        places["places_names"].append(ent[0])
                        places["places"].append(location)
                        places["emotions"].append(emotions[predicted_emotion[0]])
                        places["keywords"].append(keywords)

            return pd.Series(places)

        data = pd.read_csv("/opt/airflow/dags/groups_posts.csv")

        data[["places_names", "places", "emotions", "keywords"]] = data.apply(analyze_groups_text, axis=1)

        data.to_csv("/opt/airflow/dags/processed_data/groups_posts_processed.csv")

    def process_vk_news_data():
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        nlp = stanza.Pipeline(lang="ru", processors="tokenize,ner", use_gpu=True, download_method=None, device=device)

        tokenizer = pickle.load(open("/opt/airflow/dags/models/tokenizer.pkl", 'rb'))
        model = pickle.load(open("/opt/airflow/dags/models/bert_model.pkl", 'rb'))

        kw_model = pickle.load(open("/opt/airflow/dags/models/kw_model.pkl", 'rb'))

        def analyze_city_news(row):
            text = str(row["post"])
            print(text)

            places = {"places_names": [], "places": [], "emotions": [], "keywords": []}

            text = remove_emoji(text)[:512]

            inputs = tokenizer(text, max_length=512, padding=True, truncation=True, return_tensors="pt")
            outputs = model(**inputs)
            predicted = torch.nn.functional.softmax(outputs.logits, dim=1)
            predicted_emotion = torch.argmax(predicted, dim=1).numpy()

            doc = nlp(text)
            entities = [[ent.text, ent.type] for sent in doc.sentences for ent in sent.ents]

            try:
                keywords = kw_model.extract_keywords(text, keyphrase_ngram_range=(1, 3),
                                                 stop_words=stopwords.words("russian"))
            except:
                return "Undefined"


            for ent in entities:
                if ent[1] == "LOC" or ent[1] == "ORG":
                    if ent[0].lower() != city_name.lower() and ent[0].lower() != "россия":
                        location = geocode_place(ent[0])
                        places["places_names"].append(ent[0])
                        places["places"].append(location)
                        places["emotions"].append(emotions[predicted_emotion[0]])
                        places["keywords"].append(keywords)

            return pd.Series(places)

        data = pd.read_csv("/opt/airflow/dags/city_posts.csv")

        data[["places_names", "places", "emotions", "keywords"]] = data.apply(analyze_city_news, axis=1)

        data.to_csv("/opt/airflow/dags/processed_data/city_news_processed.csv")

    def process_vk_photos_data():
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        nlp = stanza.Pipeline(lang="ru", processors="tokenize,ner", use_gpu=True, download_method=None, device=device)

        tokenizer = pickle.load(open("/opt/airflow/dags/models/tokenizer.pkl", 'rb'))
        model = pickle.load(open("/opt/airflow/dags/models/bert_model.pkl", 'rb'))

        kw_model = pickle.load(open("/opt/airflow/dags/models/kw_model.pkl", 'rb'))

        def analyze_city_photos(row):
            text = str(row["post"])
            print(text)

            places = {"places_names": [], "places": [], "emotions": [], "keywords": []}

            text = remove_emoji(text)

            inputs = tokenizer(text, max_length=512, padding=True, truncation=True, return_tensors="pt")
            outputs = model(**inputs)
            predicted = torch.nn.functional.softmax(outputs.logits, dim=1)
            predicted_emotion = torch.argmax(predicted, dim=1).numpy()

            doc = nlp(text)
            entities = [[ent.text, ent.type] for sent in doc.sentences for ent in sent.ents]

            try:
                keywords = kw_model.extract_keywords(text, keyphrase_ngram_range=(1, 3),
                                                     stop_words=stopwords.words("russian"))
            except:
                return "Undefined"

            for ent in entities:
                if ent[1] == "LOC" or ent[1] == "ORG":
                    if ent[0].lower() != city_name.lower() and ent[0].lower() != "россия":
                        location = geocode_place(ent[0])
                        places["places_names"].append(ent[0])
                        places["places"].append(location)
                        places["emotions"].append(emotions[predicted_emotion[0]])
                        places["keywords"].append(keywords)

            return pd.Series(places)

        data = pd.read_csv("/opt/airflow/dags/city_photos.csv")

        data[["places_names", "places", "emotions", "keywords"]] = data.apply(analyze_city_photos, axis=1)

        data.to_csv("/opt/airflow/dags/processed_data/city_photos_processed.csv")

    def process_yandex_data():
        tokenizer = pickle.load(open("/opt/airflow/dags/models/tokenizer.pkl", 'rb'))
        model = pickle.load(open("/opt/airflow/dags/models/bert_model.pkl", 'rb'))

        kw_model = pickle.load(open("/opt/airflow/dags/models/kw_model.pkl", 'rb'))

        def analyze_yandex_text(row):
            text = str(row["review"])

            places = {"emotions": [], "keywords": []}

            text = remove_emoji(text)

            inputs = tokenizer(text, max_length=512, padding=True, truncation=True, return_tensors="pt")
            outputs = model(**inputs)
            predicted = torch.nn.functional.softmax(outputs.logits, dim=1)
            predicted_emotion = torch.argmax(predicted, dim=1).numpy()

            try:
                keywords = kw_model.extract_keywords(text, keyphrase_ngram_range=(1, 3),
                                                     stop_words=stopwords.words("russian"))
            except:
                return "Undefined"

            places["emotions"].append(emotions[predicted_emotion[0]])
            places["keywords"].append(keywords)

            return pd.Series(places)

        data = pd.read_csv("/opt/airflow/dags/processed_data/buildings_with_addresses.csv")

        data[["emotions", "keywords"]] = data.apply(analyze_yandex_text, axis=1)

        data.to_csv("/opt/airflow/dags/processed_data/processed_buildings_with_addresses.csv")


    def finish():
        print("FINALLY DONE!")


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

    process_vk_photos_task = PythonOperator(
        task_id="process_vk_photos",
        python_callable=process_vk_photos_data,
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


(init_models_task >> process_vk_groups_task >>
 process_vk_news_task >> process_vk_photos_task >> process_yandex_task
 >> finish_task)
