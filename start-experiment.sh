#!/bin/bash

# $1 is password
# $2 is consistency level
# $3 is num of clients

runProject() {
  # Remove old log files on each server.
  for ((i=0; i<5; i++)); do
    server="xcnc$((20 + $i % 5))"
    sshpass -p $1 ssh cs4224j@$server.comp.nus.edu.sg "source .bash_profile; cd cass && rm -rf log && mkdir log" 
    echo "Remove old logs"
  done
  
  # $1 is consistencyLevel
  # $2 is number of clients
  
  #iterate through each client and assign to the correct server to run
  for ((i=1; i<=$3; i++)); do
  	server="xcnc$((20 + $i % 5))"
  	echo "Assign client $i on $server"
  	
  	input_file="src/main/java/DataSource/xact-files/${i}.txt"
  	stdout_file="log/${i}.out.log"
  	stderr_file="log/${i}.err.log"
  	sshpass -p $1 ssh cs4224j@$server.comp.nus.edu.sg "source .bash_profile; cd cass && java -Xms2g -Xmx2g -cp target/*:target/dependency/*:. Main ${2} $4 $5 $6 $7 $8 < ${input_file} > ${stdout_file} 2> ${stderr_file} &" > /dev/null 2>&1 &
  	
  	echo "Finish running $i transaction file on $server"
  done

  ## use format below jobs stuff that didnt process, comment out above first!!
  # for ((i=38; i<=38; i++)); do
  # 	server="xcnc$((20 + $i % 5))"
  # 	echo "Assign client $i on $server"
  	
  # 	input_file="src/main/java/DataSource/xact-files/${i}.txt"
  # 	stdout_file="log/${i}.out.log"
  # 	stderr_file="log/${i}.err.log"
  # 	sshpass -p $1 ssh cs4224j@$server.comp.nus.edu.sg "source .bash_profile; cd cass && java -Xms2g -Xmx2g -cp target/*:target/dependency/*:. Main ${2} < ${input_file} > ${stdout_file} 2> ${stderr_file} &" > /dev/null 2>&1 &
  	
  # 	echo "Finish running $i transaction file on $server"
  # done
}

buildProject() {
  for ((i=1; i<=5; i++)); do
    server="xcnc$((20 + $i % 5))"
    sshpass -p $1 ssh cs4224j@$server.comp.nus.edu.sg "source .bash_profile; cd cass && mvn clean dependency:copy-dependencies package"
    echo "Built project on $server"
  done
}

# $1: Password, $2: Consistency Level, $3 Instances, $4 $5 $6 $7 $8 servers IP Addresses
echo "Starting to build on all servers"
buildProject $1
echo "Starting to run project with $2 consistency level and $3 instances"
runProject $1 $2 $3 $4 $5 $6 $7 $8
echo "Complete experiment"