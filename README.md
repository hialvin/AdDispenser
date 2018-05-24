# Word Seeker

Insight Data Engineering Project, Boston, April - June 2018
![Screenshot](https://i.imgur.com/4uP9jlC.png)
## Overview
Word Seeker is a tool to give users word recommendations according to context. When the user want a word to describe ***sunshine***, it will give out such candidates, ***"bright sunshine", "warm sunshine", "brilliant sunshine"*** , etc. It could also help students learn english. For example, they want to find the right preposition to fill in the incomplete phrase   ***"different ___  them"*** , Word Seeker could supply them with a few candidates and their word frequency in a large corpus(see screenshot above). The more a candidate appears in a large corpus doesn't mean it is the right one, so example sentences are provided as references.
## Demo    
website: http://www.alvin-yang.me/    
slides: http://www.alvin-yang.me/slides/    
youtube: //TODO    
## Implementation
### Architecture Design
![](https://i.imgur.com/Ai8ZfdA.png)

**Datasets:**    
[Google Books](http://storage.googleapis.com/books/ngrams/books/datasetsv2.html)        
Google Books project contains a large number of publish books which date back from 17 century. It is a very good corpus for frequency-based text search.    
[UMBC WebBase Corpus](https://ebiquity.umbc.edu/blogger/2013/05/01/umbc-webbase-corpus-of-3b-english-words/)    
The UMBC WebBase corpus is a dataset of high quality English paragraphs containing over three billion words derived from the Stanford WebBase project’s February 2007 Web crawl. This is where example sentences come from.    

### HBase Schema Design
![](https://i.imgur.com/HYnCvVt.png)
It is natrual to store the results in a NoSql database. I stored the queries as the partition keys, the word candidates as the column keys. Word frequency and example sentence ids are stored in the Cell. There is another table to store the mapping between the sentence id and its corresponding sentence.

## Check It Out
### Spark Tuning
During the matching example sentences with word candidates, the transformed RDD would become 20X larger than its parent RDD. If not handdled properly, there would be serious performance issue, e.g. the gabage collection time takes up to 1/3 of the total running time, and shuffle spill memory and shuffle spill disk happen. 
To solve this problem, i choosed the large executor mode. For each task, there is enough memory to execute. I also tuned the partition size (task size) and executor cores, to limit the size and the number of tasks that are concurrently executed. There are other tunnings like ‘spark.memory.fraction’ etc. After that, the gabbage collection time only took 1/8 of the execution time.
### HBase Bulk Load
HBase is a CP system, which means write would be slow because there is a concensus process.  In addition to that, there are WAL and region split, etc. So if we insert the result one by one, even in batch mode like Cassandra, it would be slow. For me, I used bulk load, which mean I generate Hfile,which is the underlying storage file hbase system. In this way, we don’t need to worry about consensus, WAL, region split etc. After bulk, we just need Hbase region server to find it and the load finished. 
The result is good, and we 

## Others
//TODO


