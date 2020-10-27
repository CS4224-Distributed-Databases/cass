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
- DataSource Folder: Since the data and transaction files are too big, we will not upload them on github. Ensure that you copy the data csv and transaction txt files to this directory locally. 
- Transactions: Contains a base transaction file + 8 Transaction queries

## Running the project
**Ensure that you have the latest code on the server**

1. Clone the latest github project into your computer
2. Ensure that you copy the data csv files into `DataSource/data-files` and transaction txt files into `DataSource/xact-files` folder locally. <br>
The files can be downloaded [here](http://www.comp.nus.edu.sg/~cs4224/project-files.zip).
3. Go to the directory of the cloned project
4. Copy the latest code to the server: If there is an existing old cass in the server, run `rm -rf cass` first to remove it. <br>
 Then run `scp –r cass cs4224j@xcnc20.comp.nus.edu.sg:~/cass`
5. Run `ls -l` and check that file permissions are `rwx------`. If it is not, run `chmod -R 700 cass`. 

**Compiling the project on the server**
1. ssh into one server `ssh cs4224j@xcnc20.comp.nus.edu.sg`
2. Run `cd cass` to enter the project directory 
3. Build the project by `mvn clean dependency:copy-dependencies package`

**Create Tables and Loading data on the server**
1. `java -Xms45g -Xmx45g -cp target/*:target/dependency/*:. InitialiseData` <br>
2. Optional: do a quick validation of the data uploaded <br>
2.1 On a new xcnc20 terminal, `cd temp/apache-cassandra-3.11.8/bin` <br>
2.2 run `./cqlsh --request-timeout="100" 192.168.48.169` <br>
2.3 `Use cs4224;` <br>
2.4 `Select count(*) from Order_line allow filtering;` should return 3746763 rows <br>
2.5 repeat for other tables <br>
>Note that step 1 drops all existing tables and create new ones


**Starting Cassandra**
1. ssh into all xcnc20-24 servers on 5 different terminals
    - ssh cs4224j@xcnc20.comp.nus.edu.sg
    - ssh cs4224j@xcnc21.comp.nus.edu.sg
    - ssh cs4224j@xcnc22.comp.nus.edu.sg
    - ssh cs4224j@xcnc23.comp.nus.edu.sg
    - ssh cs4224j@xcnc24.comp.nus.edu.sg
2. run `./start-cassandra.sh` on all of them, beginning with xcnc20 (the seed node) first. <br>
> Note: This starts cassandra in the foreground. To run cassandra in the background, run `./start-cassandra-bg.sh` instead. Remember to find and kill the process to stop cassandra later

**Running an experiment**
1. Ensure you have sshpass installed in your computer. Otherwise run `sudo apt install sshpass`
2. Locally, in the root directory of the project, run `./start-experiment.sh password consistencyLevel numOfClients`, 
replacing `password` with the password to the servers, `consistencyLevel` with `QUORUM` or `ALLONE`, `numOfClients` with 20 or 40.
3. Output written to stdout can be found in `log/i.out.log` and output written to stderr can be found in `log/i.err.log` where i is the client number.

**Generating statistics after an experiment**

*Generate the Database state*
1. run `java -Xms45g -Xmx45g -cp target/*:target/dependency/*:. EndStateRunner`
2. Open `output/end_state.csv` file
3. Manually copy the results into a row of the main `db-state.csv` which records all db end state for all experiments. 
Set the first column to be this experiment number. 

*Generate Performance and Throughput Statistics*
1. run `java -Xms45g -Xmx45g -cp target/*:target/dependency/*:. TotalStatsRunner`
2. For Throughput Statistics: <br>
2.1 Open `output/throughput_stats.csv` file <br>
2.2 Manually copy the results into a row of the main `throughput.csv` which records all min, avg and max throughputs for all experiments. 
Set the first column to be this experiment number. <br>
3. For Performance Statistics: <br>
3.1 Open `output/client_stats.csv` file <br>
3.2 Manually copy the results into a row of the main `clients.csv` which records all clients statistics for all experiments.  <br>

## Optional Local Set up instructions
- Clone this project into an IDLE of your choice
- Configure Maven to handle the dependencies required

Creating a local cluster to test
- To create cluster with 3 nodes on local cassandra
`ccm create local -v 2.0.5 -n 3 -s`

- Launch cqlsh
`ccm node1 cqlsh`

- Use the created keyspace
`USE CS4224;`

- To remove cluster and keyspace and nodes
`ccm remove`

## Helpful commands
1. To find and kill a background process <br>
1.1 `ps -ef | head -1; ps -ef | grep cass` to list all cass related processes <br>
1.2 identify the pid of the process you want to kill <br>
1.3 `kill -9 pidNum` with pidNum being the pid you identified <br>

2. If facing some corrupted SSLTable issues and not able to start cassandra on some node <br>
2.1 Read the error message <br>
2.2 Go to the data directory to `rm md-NO-.db` with NO being the number that was reported in the error message <br>
2.3 Restart the failing node <br>
2.4 Then run `./nodetool repair -full cs4224` <br>

## Notes about Cassandra
- Cassandra will order the partition keys and the clustering keys (ordered by their precedence in the PRIMARY KEY definition), and then the columns follow in ascending order. Hence the ordering of the columns in createTable is not followed.