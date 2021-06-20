FROM python:3.8

RUN apt-get update
RUN pip install --upgrade pip

# install additional dependencies (might have duplicates?)
# (was pipenv previously but had problems with alpine)
RUN mkdir /project

COPY ./requirements.txt /project/requirements.txt
RUN pip install -r /project/requirements.txt

# copy whole installation (minus dockerignore)
COPY . /project/

# set workdir to have subscripts in scope
WORKDIR /project
CMD python main.py
