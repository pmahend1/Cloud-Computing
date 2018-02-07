# Cloud-Computing
This repository has all of my cloud computing related projects source code.  
  * Hadoop MapReduce  
  * Spark  

## Hadoop - Map Reduce projects
### [1. DocWordCount](https://github.com/pmahend1/Cloud-Computing/tree/master/DocWordCount)
DocWordCount.java outputs the word count for each distinct word in each file. Output will in the form 'word#####filename count' where '#####' is the delimiter.

  #### Execution : 
  argument 1 : input directory where files are stored.  
  argument 2 : output directory.

### [2. TermFrequency](https://github.com/pmahend1/Cloud-Computing/tree/master/TermFrequency)
TermFrequency.java outputs term frequency(TF) for each word in the corpus in the format 'word#####filename TF' where ##### is delimiter

TF(t,d) = No. of times term t appears in document d

TF=1 + log<sub>10</sub> (TF(t,d)) 
 
  #### Execution : 
  argument 1 : input directory where files are stored.  
  argument 2 : output directory.  


### [3. TFIDF](https://github.com/pmahend1/Cloud-Computing/tree/master/TFIDF)
TFIDF.java calculates Term Frequency for each word in corpus(TF) and Inverse Document Frequency(IDF) for each word and then outputs TF-IDF in the format 'word#####filename TFIDF' where ##### is delimiter.

TF=1 + log<sub>10</sub>(TF(t,d))  

IDF= log<sub>10</sub> (Total no. of documents / No. of documents containing term t)

  #### Execution : 
  argument 1 : input directory.  
  argument 2 : output directory.  


### [4. BasicSearchEngine](https://github.com/pmahend1/Cloud-Computing/tree/master/BasicSearchEngine)
Basic query search engine that takes user query and outputs list of documents that matches the query in the format 'filename TFIDFWeightSum' and input to mapper is output of TFIDF.java
 
  #### Execution : 
  argument 1 : input directory.  
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**Note:** Give the output files' directory of TFIDF.java as input directory.  
  argument 2 : output directory.  
  
### [5. PageRank](https://github.com/pmahend1/Cloud-Computing/tree/master/PageRank)
Given a graph of hyper-links with out-links from one web page to other this calculates page rank and outputs in descending order of the rank.
 
  #### Execution : 
  hadoop jar PageRank.jar edu.cloud.prateek.Driver argument_1 argument_2
  
  argument_1 : input directory.  
  argument_2 : output directory.  
  
  ### [6. Linear Regression](https://github.com/pmahend1/Cloud-Computing/tree/master/Linear%20Regression)
Python-Spark program for finding beta co-efficients of linear regression by computing summation form of closed form expression: β<sup>^</sup>=(X<sup>T</sup>X)<sup>−1</sup>X<sup>T</sup>Y 

  #### Execution : 
  spark-submit linreg.py argument_1
  
  argument_1 : input file name  
   
