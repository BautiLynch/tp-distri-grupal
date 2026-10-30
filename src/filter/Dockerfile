FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app

COPY common ./common
COPY filter ./filter

ENTRYPOINT ["python3", "-m", "filter.main"]
