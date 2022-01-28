FROM python

RUN apt install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /ingest-app
COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python3", "ingest_data.py"]