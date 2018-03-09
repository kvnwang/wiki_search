# Wiki Search Engine
## Overview
Parallel and distributed querying system over large Wikipedia Datasets
- Java Implmentation
- Hadoop
- Spark

## Descrtiption
## Inverted Index [Lab 2: Due March 2nd
- Implement a simple inverted index using MapReduce.
- For this, you can use the inverted index you build at Lab 2. You can pick any implementation from
the group members.

## Inverted Index + Stop & Scrub Word Elimination (Due March 16th)
- Write a MapReduce program to identify stop words in a given dataset. You can use the Word Count
program that was presented in the lab tutorial. You will need to find a good threshold for word count
frequency such that words appearing more than this threshold will be characterized as stop words.
- Write a MapReduce program to identify scrub words in a given dataset.
- Modify your inverted index program to eliminate stop and scrub words. Please note that you can use
multiple levels of MapReduce programs, the design is up to you. How much reduction (in bytes, on
the complete dataset) have you achieved in the size of your index after eliminating stop words and
scrub words.
- For this assignment, you will need to run your code on the full dataset on the cluster.

## Inverted Index + Stop & Scrub Words + Document Positions (Due March 29th)
- Extend your inverted index program with stop and scrub word elimination into a full inverted index.
It is up to you how to specify a word’s position in a document.

## Querying with Hadoop (Due April 13th)
- Write a query program which, given a boolean combination of words (in conjunctive normal form)
as described above, returns the matching document id’s as well as text snippets from each document
showing where the query term appears in the document. Since the inverted index can be large, use a
SPARK-based algorithm for your query program.

## Web Interface (Due April 20th)
- Design and implement a simple web-based GUI for your querying system which takes in the query and
returns the result in a nicely-formatted form.


## Project Report (Due April 25th)
Write a short project report describing the design and implementation of your system, as well as some
measurements of the performance of your system (how long does index creation or querying take?). Please
include a discussion section at the end that comments on your whole experience with this assignment, any
lessons learned, or any issues left as open questions.
