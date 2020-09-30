## CS4224 Distributed Databases
*AY2020/2021 Semester 1*, *School of Computing*, *National University of Singapore*

## Team members
- [Ivan Ho](https://github.com/ihwk1996)
- [Kerryn Eer](https://github.com/KerrynEer)
- [Ooi Hui Ying](https://github.com/ooihuiying)
- [Wayne Seah](https://github.com/wayneswq)

## Project Summary
This learning tasks for this project are 
- Install a distributed database system on a cluster of machines
- Design a data model and implement transactions to support an application
- Benchmark the performance of an application

## Project Structure
The bulk of our code are in the folder src -> main -> java
- DataLoader Folder: Contains the code to load the data into the Cassandra database and create table schemas
- DataSource Folder: Since the csv files are too big, we will not upload them on github. Ensure that you copy the data csv files to this directory locally. 
- Transactions: Contains a base transaction file + 8 Transaction queries

## Set up instructions
(TODO)
- Clone this project into an IDLE of your choice
- Configure Maven to handle the dependencies required

(for localhost)
- To create cluster with 3 nodes on local cassandra
`ccm create local -v 2.0.5 -n 3 -s`
//TODO use Apache Cassandra 3.11.6 as stated in project, not sure if works for this version

- Launch cqlsh
`ccm node1 cqlsh`

- Use the created keyspace
`USE CS4224;`

- To remove cluster and keyspace and nodes
`ccm remove`

## Notes about Cassandra
- Cassandra will order the partition keys and the clustering keys (ordered by their precedence in the PRIMARY KEY definition), and then the columns follow in ascending order. Hence the ordering of the columns in createTable is not followed.