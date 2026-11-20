FROM python:3-alpine

RUN pip3 install docker

COPY chaos-monkey .

ENTRYPOINT ["python3", "main.py"]
