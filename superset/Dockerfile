FROM apache/superset

USER root

COPY --chown=superset entrypoint.sh /entrypoint.sh
RUN chmod a+x /entrypoint.sh
ENV SUPERSET_CONFIG_PATH /app/superset_config.py
COPY --chown=superset superset_config.py /app/superset_config.py

RUN pip install --upgrade pip
RUN pip install clickhouse-connect
RUN mkdir /metadata
RUN chown superset /metadata

USER superset
RUN superset db upgrade
ENV FLASK_APP=superset
ENV FLASK_RUN_HOST=0.0.0.0
RUN superset fab create-admin --username admin --firstname admin --lastname admin --email admin@platon.io --password t0ps3cr3t
RUN superset load_examples
RUN superset init
RUN superset set_database_uri -d platon-db -u clickhousedb://localhost/default

ENTRYPOINT /entrypoint.sh
