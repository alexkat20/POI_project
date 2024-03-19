FROM python:3.9-slim-bullseye

ENV PYTHONUNBUFFERED=1 PATH=/venv/bin:$PATH

WORKDIR /app

RUN apt-get update \
      && apt-get upgrade -y \
      && apt-get install -y gcc default-libmysqlclient-dev pkg-config procps \
      && rm -rf /var/lib/apt/lists/*


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8087

CMD python main.py

USER root
