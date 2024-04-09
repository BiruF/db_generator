FROM postgres

ENV POSTGRES_PASSWORD postgres
ENV POSTGRES_DB generator

COPY generator_dump.sql /docker-entrypoint-initdb.d/
