# Wiki Search Engine

# Group Information
  + Group 3 
  + Team Name - Hadooplahoops

## Overview
Final Project for COSI 132: Parallel and distributed querying system over large Wikipedia Datasets
## Authors:
+ Kevin Wang, Anthony Liu, Geoffrey Kao, Christine Zhu

## Implementation and Technologies:
+ Java
+ MapReduce
+ Hadoop
+ Spark
+ Spring Boot

## Project Overview 
This is a web search interface that allows users to search through KBs of Wikipedia data. This project uses Hadoop MapReduce to index all wikipedia articles found in wikipedia .csv files with information of each word, article id, url, and document position. The MapReduce outputs json string structures to genrate the inverted index for each word. After using MapReduce to index all the articles, Spark and Spring Boot are used to search through all indexed articles on HDFS or locally to ouptput all articles and text snippets of queried words in conjunctive normal form. 

## Getting Started Locally
+ Run Hadoop MapReduce job
  + Run Driver.java class to index wikipedia .csv files for the MapReduce job
  + Run mvn clean install, then yarn jar app.jar 
+ Run Spark and Spring Web Interface 
  + Run 'mvn clean install'
  + Run run java -jar {app_name}.jar
  + The application should be running on port number 8000
