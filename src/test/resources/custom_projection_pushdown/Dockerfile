FROM postgres:10.2
ENV POSTGRES_PASSWORD=root
ENV POSTGRES_USER=root
ENV POSTGRES_DB=spark_test

COPY queries.sql /docker-entrypoint-initdb.d/queries.sql
