#!/bin/bash

server=$(hostname -s)
echo "Starting cassandra on $server"
cd temp/apache-cassandra-3.11.8/
bin/cassandra -f -D cassandra.config=conf/$server/cassandra.yaml
