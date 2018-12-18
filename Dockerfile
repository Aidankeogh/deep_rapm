FROM continuumio/anaconda3

COPY requirements.txt /
RUN pip install -r requirements.txt

RUN mkdir /rapm-model/
WORKDIR /rapm-model/
COPY . /rapm-model/