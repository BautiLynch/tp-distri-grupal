FROM python:3.10-alpine

RUN pip3 install pika

COPY common ./common
COPY splitter .

ENTRYPOINT ["python3", "main.py"]
