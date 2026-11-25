FROM python:3-alpine

RUN pip3 install pika docker

COPY common ./common
COPY monitor .

ENTRYPOINT ["python3", "main.py"]
