FROM python:3.8-slim-buster

WORKDIR .

# Python modules
RUN pip3 install requests pandas wget

# Docker host time
ENV TZ=America/Lima
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezoneCOPY . .

COPY . .
CMD [ "python3", "main.py"]