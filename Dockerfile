
#
# Column file ingest requires Postgres to run a python program
#   with the Copy From Program SQL command.
# So, this Docker image for data import includes:
#   Postgres
#   AdsDataSqlSync code
#   the ability to run python.

FROM postgres:9.5

RUN apt-get update
RUN apt-get install -y python-pip python-dev
RUN apt-get install -y python-psycopg2
RUN apt-get install -y libpq-dev
RUN pip install --upgrade pip

ADD . /var/app/current
RUN pip install -r /var/app/current/requirements.txt

#put source code from this repo in container so we can run it
ADD . /var/app/current
RUN pip install -r /var/app/current/requirements.txt
