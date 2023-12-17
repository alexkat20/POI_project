FROM apache/airflow:2.7.1-python3.9
LABEL authors="alexk"

WORKDIR /usr/src/app

COPY requirements.txt ./

USER airflow

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r requirements.txt

USER root

COPY . .
