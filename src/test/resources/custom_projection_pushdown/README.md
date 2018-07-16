# Runs PostgresQL 10.2 as a Docker container

To run, please execute in order:
```
python data_generator.py
docker build -t psql_json_test .
docker run -p 5432:5432  psql_json_test
# After the image starts correctly
docker exec -it <you container number> bash .
```

If you want to setup more explicit logging, you can follow the steps defined here: [How to log PostgreSQL queries ?](https://stackoverflow.com/questions/722221/how-to-log-postgresql-queries) and execute, inside the container:
```
echo "log_statement = 'all'" >> /var/lib/postgresql/data/postgresql.conf
echo "log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log'" >> /var/lib/postgresql/data/postgresql.conf
echo "log_directory = '/tmp'" >> /var/lib/postgresql/data/postgresql.conf
echo "logging_collector = on" >> /var/lib/postgresql/data/postgresql.conf
echo "log_statement = 'all'" >> /var/lib/postgresql/data/postgresql.conf
```

After that you need to `docker restart <you container number>` and reconnect again in bash. The log files should be  located in /tmp directory

If you want to insert 1 000 000 rows to make some tests, execute `python data_generator.py` and start new container.