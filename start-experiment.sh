#!/bin/bash

# $1 is password
# $2 is consistency level
# $3 is num of clients

runProject() {
  # Remove old log files on each server.
  for ((i=0; i<5; i++)); do
    server="xcnd$((20 + $i % 5))"
    sshpass -p $1 ssh cs4224j@$server.comp.nus.edu.sg "cd cass && rm -rf log && mkdir log" 
    echo "Remove old logs"
  done
  
  # $1 is consistencyLevel
  # $2 is number of clients
  
  #iterate through each client and assign to the correct server to run
  for ((i=1; i<=$3; i++)); do
	server="xcnd$((20 + $i % 5))"
	echo "Assign client $i on $server"
	
	input_file="src\main\java\DataSource\xact-files\$i.txt"
	stdout_file="log/$i.out.log"
	stderr_file="log/$i.err.log"
	echo "commented out running"
	sshpass -p $1 ssh cs4224j@$server.comp.nus.edu.sg "cd cass && java -Xms45g -Xmx45g -cp target/*:target/dependency/*:. Main ${2} < ${input_file} > ${stdout_file} 2> ${stderr_file} &" > /dev/null 2>&1 &
	
	echo "Finish running $i transaction file on $server"
  done
}

buildProject() {
  for ((i=1; i<=5; i++)); do
    server="xcnd$((20 + $i % 5))"
    sshpass -p $1 ssh cs4224j@$server.comp.nus.edu.sg "cd cass && mvn clean dependency:copy-dependencies package"
    echo "Built project on $server"
  done
}

echo "Starting to build on all servers"
buildProject $1
echo "Starting to run project with $2 consistency level and $3 instances"
runProject $1 $2 $3
echo "Complete experiment"