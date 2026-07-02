FROM apache/superset:4.0.2

USER root
RUN pip install --no-cache-dir \
      trino==0.329.0 \
      sqlalchemy-trino==0.5.0 \
      psycopg2-binary==2.9.9

COPY conf/superset/bootstrap.sh /app/bootstrap.sh
RUN chmod +x /app/bootstrap.sh

USER superset
ENTRYPOINT ["/app/bootstrap.sh"]
