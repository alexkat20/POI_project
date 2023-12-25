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
    stanza.download("ru")

    nlp = stanza.Pipeline(lang="ru", processors="tokenize,ner")

    nltk.download("stopwords")
    kw_model = KeyBERT()

    emotions = {0: "NEUTRAL", 1: "POSITIVE", 2: "NEGATIVE"}

    city_name = "Новокуйбышевск"

    def get_keywords(text):
        print(text)
        try:
            return kw_model.extract_keywords(text, keyphrase_ngram_range=(1, 3), stop_words=stopwords.words("russian"))
        except:
            return "Undefined"

    def stanza_nlp_ru(text):
        doc = nlp(text)
        return [[ent.text, ent.type] for sent in doc.sentences for ent in sent.ents]

    def predict_emotion(text):
        import torch
        from transformers import AutoModelForSequenceClassification
        from transformers import BertTokenizerFast

        tokenizer = BertTokenizerFast.from_pretrained("blanchefort/rubert-base-cased-sentiment")
        model = AutoModelForSequenceClassification.from_pretrained(
            "blanchefort/rubert-base-cased-sentiment", return_dict=True
        )

        inputs = tokenizer(text, max_length=512, padding=True, truncation=True, return_tensors="pt")
        outputs = model(**inputs)
        predicted = torch.nn.functional.softmax(outputs.logits, dim=1)
        predicted = torch.argmax(predicted, dim=1).numpy()
        return predicted

    def geocode_place(place):
        geolocator = ArcGIS(user_agent="POI")

        location = geolocator.geocode(f"{city_name} {place}", timeout=10)

        return location

    def analyze_text(row):
        text = row["post"]

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

    def analyze_yandex_text(row):
        text = row["review"]

        places = {"emotions": [], "keywords": []}

        text = remove_emoji(text)

        predicted_emotion = predict_emotion(text)

        keywords = get_keywords(text)

        places["emotions"].append(emotions[predicted_emotion[0]])
        places["keywords"].append(keywords)

        return pd.Series(places)

    def process_vk_groups_data():
        data = pd.read_csv("/opt/airflow/dags/groups_posts.csv")

        data[["places_names", "places", "emotions", "keywords"]] = data.apply(analyze_text, axis=1)

        data.to_csv("/opt/airflow/dags/processed_data/groups_posts_processed.csv")

    def process_vk_news_data():
        data = pd.read_csv("/opt/airflow/dags/city_posts.csv")

        data[["places_names", "places", "emotions", "keywords"]] = data.apply(analyze_text, axis=1)

        data.to_csv("./opt/airflow/dags/processed_data/city_news_processed.csv")

    def process_vk_photos_data():
        data = pd.read_csv("/opt/airflow/dags/city_photos.csv")

        data[["places_names", "places", "emotions", "keywords"]] = data.apply(analyze_text, axis=1)

        data.to_csv("/opt/airflow/dags/processed_data/city_photos_processed.csv")

    def process_yandex_data():
        data = pd.read_csv("/opt/airflow/dags/processed_data/buildings_with_addresses.csv")

        data[["emotions", "keywords"]] = data.apply(analyze_yandex_text, axis=1)

        data.to_csv("/opt/airflow/dags/processed_data/processed_buildings_with_addresses.csv")


    def finish():
        print("DONE!")

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


([process_vk_groups_task, process_vk_news_task, process_vk_photos_task, process_yandex_task] >> finish_task)
