FROM python:latest
RUN mkdir /server
WORKDIR /server
COPY requirements.txt /server/
RUN python -m pip install -r requirements.txt
ADD . /server/