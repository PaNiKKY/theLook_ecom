# FROM apache/airflow:2.9.2-python3.11
FROM apache/airflow:slim-2.10.5-python3.12

USER root

# Install dependencies
RUN apt-get update \
  && apt install unzip -y\
  && apt-get install -y --no-install-recommends \
         vim gnupg2 libnss3-dev wget openjdk-17-jdk\
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# RUN export JAVA_HOME

COPY requirements.txt /opt/airflow/requirements.txt

USER airflow

# RUN pip install --upgrade pip
RUN pip3 install --no-cache-dir -r requirements.txt

RUN pip3 install --no-cache-dir --use-deprecated=legacy-resolver apache-airflow-providers-google