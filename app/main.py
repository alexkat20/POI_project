from fastapi import FastAPI
import uvicorn
import stanza
import nltk
from nltk.corpus import stopwords
from keybert import KeyBERT
from transformers import AutoModelForSequenceClassification
from transformers import BertTokenizerFast
import torch

app = FastAPI(openapi_url="/api/v1/openapi.json", docs_url="/api/v1/docs", redoc_url="/api/v1/redocs",
              swagger_ui_oauth2_redirect_url="/api/v1/docs/oauth2-redirect")

try:
    stanza.download("ru")
except:
    print("Already exists")

nltk.download("stopwords")

kw_model = KeyBERT()

tokenizer = BertTokenizerFast.from_pretrained(
    "blanchefort/rubert-base-cased-sentiment"
)
model = AutoModelForSequenceClassification.from_pretrained(
    "blanchefort/rubert-base-cased-sentiment"
)

emotions = {0: "NEUTRAL", 1: "POSITIVE", 2: "NEGATIVE"}


@app.get('/get_emotion')
def get_emotion(text: str):
    inputs = tokenizer(text, max_length=512, padding=True, truncation=True, return_tensors="pt")
    outputs = model(**inputs)
    predicted = torch.nn.functional.softmax(outputs.logits, dim=1)
    predicted_emotion = torch.argmax(predicted, dim=1).numpy()
    return {'emotion': int(predicted_emotion[0])}


@app.get('/extract_geo_entities')
def extract_geo_entities(text):
    device = "cuda" if torch.cuda.is_available() else "cpu"
    nlp = stanza.Pipeline(lang="ru", processors="tokenize,ner", use_gpu=True, download_method=None, device=device)
    doc = nlp(text)
    entities = [[ent.text, ent.type] for sent in doc.sentences for ent in sent.ents]

    return {"entities": entities}


@app.get('/get_keywords')
def get_keywords(text):
    try:
        keywords = " ".join([
            kw[0]
            for kw in kw_model.extract_keywords(
                text, keyphrase_ngram_range=(1, 3), stop_words=stopwords.words("russian")
            )
        ])
    except:
        return "Undefined"

    return {"keywords": keywords}


if __name__ == "__main__":
    uvicorn.run("main:app", port=8087, host="127.0.0.1", log_level='error')
