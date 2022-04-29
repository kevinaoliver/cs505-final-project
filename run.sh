#!/bin/bash

#Compile code with Maven, produceds target directory
mvn clean package

#Creates Docker container, needs file named Dockerfile to create it, done separately
sudo docker build -t cs505-final .

#Run Docker container 
sudo docker run -d --network="host" --rm -p 9000:9000 cs505-final
#sudo docker run -it --network="host" --rm -p 8082:8082 cs505-final
