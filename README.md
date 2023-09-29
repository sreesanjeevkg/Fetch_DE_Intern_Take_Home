# Real-Time and OLAP Data Processing with Kafka

## Project Overview

Fetch Data Engineer Take Home Assessment : This project demonstrates a real-time and OLAP (Online Analytical Processing) data processing pipeline using Apache Kafka. It includes two separate consumer programs for real-time and OLAP data processing, as well as a common helper class for Kafka-related operations.

## Project Architecture

![kafka](https://github.com/sreesanjeevkg/fetch_de_take_home/assets/32449066/6e706d61-ad94-4fe9-bfdd-2237691ef9f8)

### Features

- Real-time data processing from a Kafka topic.
- OLAP data processing with the ability to transform and aggregate data.
- Writing processed data to another Kafka topic for further analysis.
- Centralized Kafka configuration management.

## Prerequisites

Before running this project, ensure that you have the following prerequisite installed and configured:


- git
- docker
  - Windows - https://medium.com/devops-with-valentine/how-to-install-docker-on-windows-10-11-step-by-step-83074a80e6f9
  - Mac - https://medium.com/featurepreneur/setting-up-docker-on-a-mac-2d3ab93801e4
          

## Installation

1. Clone the repository to your local machine: git clone https://github.com/sreesanjeevkg/fetch_de_take_home.git
2. Once Cloned, Run the dockercompose.yml file using the docker desktop
     - It will set up the Zookeeper and Kafka Server. It will also spin up a kafka producer which messages a topic called 'user-login' every second
3. Run oltp, olap python programs - pass the arguments and config according to ur prefernce TODO: Make changes here

## Design Choices and why i choose them

**Seperation of Consumers :** The Consumers have been seperated for real-time and OLAP processing. The main reason for this design choice was performance isolation, by separating real-time and OLAP processing, we reduce the risk of performance degradation in the real-time consumer due to resource-intensive OLAP operations. This design choice helps maintain a responsive real-time processing pipeline.

**Replication Factor :** The Kafka topic "processed-user-login-data has a replication factor of 2 so that even if 1 broker goes down it can replicate itself in another one

## How would you deploy this application in production?

** Using docker or kubernetes for deployment : ** We can package all our code to a docker image for a efficient production pipeline.

## What other components would you want to add to make this production ready?


**Partitions based on a specific key / Dynamic Partitions : ** In production we could have partitions based on specific attributes maybe like locale or type of device, it can have wide range of benefits while reading or any other analysis tasks. We can also try Dynamic partitions so that there are no unnecessary skew in our partitions.


## How can this application scale with a growing dataset?

**More than 1 consumer in the consumer group : ** The "user-login" producer produces to just 1 partition , which means we can have only 1 consumer. We should make to deliver it to multiple partitions so that we can make use of the "distributed" kakfka consumers to consume messages


