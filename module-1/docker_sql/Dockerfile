FROM python:3.8

RUN apt-get install wget -y

RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app

COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python3", "ingest_data.py"]

#commands
## docker build -t taxi_ingest:v001 .
## docker run -it taxi_ingest:v001
