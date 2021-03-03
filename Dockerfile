FROM python:3.8-slim-buster

WORKDIR .

COPY data/ data/
RUN pip3 install requests pandas wget

COPY . .

CMD [ "python3", "main.py"]