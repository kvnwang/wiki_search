# Wiki Search Engine

# Group Information
  + Group 3 
  + Team Name - Hadooplahoops

## Authors
+ Kevin Wang, Anthony Liu, Geoffrey Kao, Christine Zhu

## Implementation and Technologies
+ Java
+ MapReduce
+ Hadoop
+ Spark
+ Spring Boot

## Project Overview 
This is a web search interface that allows users to search through KBs of Wikipedia data. This project uses Hadoop MapReduce to index all wikipedia articles found in wikipedia .csv files with information of each word, article id, url, and document position. The MapReduce outputs json string structures to genrate the inverted index for each word. After using MapReduce to index all the articles, Spark and Spring Boot are used to search through all indexed articles on HDFS or locally to ouptput all articles and text snippets of queried words in conjunctive normal form. 

This project uses Java as the primary programming languaage. We use Hadoop MapReduce frmawork to index wiki articles, Spark to search teh indexed articles, and Spring Boot as the web framwork to connect with Spark to issue Spark jobs to search indexed articles on HDFS. We also use HTML, CSS, and JS as the front-end to render web paages with Spring Boot.

## Getting Started Locally
+ Run Hadoop MapReduce job
  + Run Driver.java class to index wikipedia .csv files for the MapReduce job
  + Run mvn clean install, then yarn jar app.jar 
+ Run Spark and Spring Web Interface 
  + Run 'mvn clean install'
  + Run run java -jar {app_name}.jar
  + The application should be running on port number 8000
