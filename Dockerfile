FROM bluelens/python:3.6
MAINTAINER bluehackmaster <master@bluehack.net>

RUN mkdir -p /usr/src/app

WORKDIR /usr/src/app

COPY . /usr/src/app

RUN pip install -r requirements.txt
RUN pip install --upgrade google-cloud-bigquery

CMD ["python3", "main.py"]
