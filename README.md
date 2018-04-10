# Wiki Search Engine
# Group 3 (cs132g3) - Hadooplahoops 

# Group 3 (cs132g3) - Hadooplahoops
## Overview
Final Project for COSI 132: Parallel and distributed querying system over large Wikipedia Datasets
## Authors:
- Kevin Wang
- Anthony Liu
- Geoffrey Kao
- Christine Zhu

## Implementation and Technologies:
- Java
- MapReduce
- Hadoop
- Spark


## Descrtiption
## [x] March 2nd: Inverted Index
- Implement a simple inverted index using MapReduce.
- For this, you can use the inverted index you build at Lab 2. You can pick any implementation from
the group members.

<<<<<<< HEAD
## [] March 16th: Inverted Index + Stop & Scrub Word Elimination
=======
## [x] March 16th: Inverted Index + Stop & Scrub Word Elimination
>>>>>>> 976b5d44fd32c37e945a941fd8f8492bdcf3c61a
- Write a MapReduce program to identify stop words in a given dataset. You can use the Word Count
program that was presented in the lab tutorial. You will need to find a good threshold for word count
frequency such that words appearing more than this threshold will be characterized as stop words.
- Write a MapReduce program to identify scrub words in a given dataset.
- Modify your inverted index program to eliminate stop and scrub words. Please note that you can use
multiple levels of MapReduce programs, the design is up to you. How much reduction (in bytes, on
the complete dataset) have you achieved in the size of your index after eliminating stop words and
scrub words.
- For this assignment, you will need to run your code on the full dataset on the cluster.

## [] March 29th: Inverted Index + Stop & Scrub Words + Document Positions
- Extend your inverted index program with stop and scrub word elimination into a full inverted index.
It is up to you how to specify a word’s position in a document.

## [] April 13th: Querying with Hadoop
- Write a query program which, given a boolean combination of words (in conjunctive normal form)
as described above, returns the matching document id’s as well as text snippets from each document
showing where the query term appears in the document. Since the inverted index can be large, use a
SPARK-based algorithm for your query program.

## [] April 20th:  Web Interface
- Design and implement a simple web-based GUI for your querying system which takes in the query and
returns the result in a nicely-formatted form.


## [] April 25th: Project Report
- Write a short project report describing the design and implementation of your system, as well as some
measurements of the performance of your system (how long does index creation or querying take?). Please
include a discussion section at the end that comments on your whole experience with this assignment, any
lessons learned, or any issues left as open questions.
