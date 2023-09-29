# Real-Time and OLAP Data Processing with Kafka

## Project Overview

This project demonstrates a real-time and OLAP (Online Analytical Processing) data processing pipeline using Apache Kafka. It includes two separate consumer programs for real-time and OLAP data processing, as well as a common helper class for Kafka-related operations.

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
3. Run oltp, olap python programs - pass the arguments and config according to ur prefernce

## Design Choices and why i choose them
