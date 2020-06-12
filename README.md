# Data-Intensive-Distributed-Computing
Big data processing frameworks (Spark, Hadoop), programming in Java and Scala, and numerous practical and well-known algorithms

#### This repository contains the Java and Scala implementations of the course project and the assignments of the data intensive distributed computing course (CS 651) at the University of Waterloo, Winter 2020. They cover a broad range of data intensive distributed problems such as text data analysis, graph data analysis. machine learning, GraphX library and distributed Graph RDD processing, and so on. In the following sections, I will describe the components of this work in more details.


### Assignment 4
Multi-Source Personalized PageRank: performs Personalized PageRank (extension of PageRank algorithm where the nodes are ranked based on their (explicit or implicit) connections/interactions with a source node) for multiple source nodes simultaneously. (In Hadoop)

### Assignment 3
Inverted Indexing a body of text document and vocabulary and performing Information Retrieval (IR) on the indices. The codes are in Java/Hadoop

### Assignment 1
Written in Java/Hadoop, the goal is to compute the [Point-wise Mutual Information (PMI)](https://en.wikipedia.org/wiki/Pointwise_mutual_information), which is an association measure. PairsPMI and StripesPMI are two different implemenations of this algorithm in this repo.

### Assignment 0
This one has the simple algorithms of *Word Count* and *Bigram Relative Frequency* written in Hadoop/Java. It covers different types of combiners and optimizations that can enhance the performance of the distributed system by taking advantage of the local node computations.

