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
Google Books project contains a large number of publish books which date back from 17 century. It is a very good corpus for frequency-based text search. In the dataset specificly, corpus has been converted to N-gram versus their number of occurrence.
[UMBC WebBase Corpus](https://ebiquity.umbc.edu/blogger/2013/05/01/umbc-webbase-corpus-of-3b-english-words/)    
The UMBC WebBase corpus is a dataset of high quality English paragraphs containing over three billion words derived from the Stanford WebBase project’s February 2007 Web crawl. This is where example sentences come from.    

### HBase Schema Design
![](https://i.imgur.com/HYnCvVt.png)

It is natrual to store the results in a NoSql database. I stored the queries as the partition keys, the word candidates as the column keys. Word frequency and example sentence ids are stored in the Cell. There is another table to store the mapping between the sentence id and its corresponding sentence.

## Check It Out
#### Salted key approach to avoid data skew

In cryptography, a [salt](https://en.wikipedia.org/wiki/Salt_(cryptography) ) is random data that is used as an additional input to a one-way function that "hashes" data, a password or passphrase.  Here I use it to evenly distributed the keys because of the randomness of hash function like MD5. Here is how it works:

1. Hash the key using MD5 Hashing
2. Get the partition number where the key should reside by **MD5** mod **# of partitions**
3. Prepend the partition number to the key 

| **key**                  | **MD5 (Decimal system)** | **Salted key (md5 % 4)** |
| ------------------------ | ------------------------ | ------------------------ |
| **different,_,them**     | 10294754                 | 0:different,_,them       |
| **hear,_,you**           | 10928427                 | 3:hear,_,you             |
| **_ a research project** | 59382721                 | 1:_ a research project   |
| **a,_,of paper**         | 54922382                 | 2:a,_,of paper           |

#### Bulk load results into HBase by Hfile

It is inefficient to bulk load by writing through the region servers since it requires HBase to use resources such as the write-ahead log (WAL), the compaction and flush queues, server memory and will trigger a lot of garbage compaction, all of which can be bypassed as explained nicely in [this Cloudera Blog](http://blog.cloudera.com/blog/2013/09/how-to-use-hbase-bulk-loading-and-why/).

The general process involves:

1. Estimate the total size of the data, and determine the optimal number of regions in HBase.
2. Create an empty table, pre-split on the region boundaries - here we [salt](https://sematext.com/blog/2012/04/09/hbasewd-avoid-regionserver-hotspotting-despite-writing-records-with-sequential-keys/) our keys in a predictable manner to achieve this. 
3. Use a simple custom [partitioner](https://spark.apache.org/docs/1.6.2/api/java/org/apache/spark/Partitioner.html) in Spark to split the RDD to match the destination region splits. 
4. Generate the HFiles using Spark and standard Hadoop libraries.
5. Load the data into HBase using the standard HBase command line bulk load tools.

Credits: [Efficient bulk load of HBase using Spark — OpenCore](https://www.opencore.com/blog/2016/10/efficient-bulk-load-of-hbase-using-spark/)


## Others
***TODO***     
Make changes to HBase schema to avoid range query completely.
Set up a redis cluster to help pick sentences which cover most queries, and filter out the sentences that only cover a small portion of queries.


