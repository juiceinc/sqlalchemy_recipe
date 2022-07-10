FROM python:latest

WORKDIR /app

COPY ./requirements.txt /app
COPY ./requirements_dev.txt /app

RUN pip install --no-cache-dir --upgrade -r requirements_dev.txt

COPY . /app

RUN pytest