import json
import random

logs = []
insert_logs = []
for nr in range(0, 1000000):
    user_json = {"fullName": "Relation {nr}".format(nr=nr), "changes": 1,
        "important": True, "businessValue": random.randint(1, 4000)
    }
    logs.append("('user{id}', '{json}', {partition})".format(json=json.dumps(user_json),
    partition=random.randint(1, 5), id=nr))
    if nr % 100 == 0:
        insert_values = ', '.join(logs)
        insert_logs.append("INSERT INTO friends(user_name, friends_list, partition_nr) VALUES {values};".format(values=insert_values))
        logs = []

output_file = open('./queries_1_million.sql', 'w+')
insert_values = '\n '.join(insert_logs)
output_file.write("""
    CREATE TABLE friends (
      user_name VARCHAR(10) NOT NULL,
      friends_list JSONB NOT NULL,
      partition_nr INTEGER NOT NULL,
      PRIMARY KEY (user_name)
    );
    CREATE INDEX ON friends (partition_nr);

    {values}

    """.format(values=insert_values))
output_file.close()