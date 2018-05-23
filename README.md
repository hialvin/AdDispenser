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

### Hbase Schema Design
![](https://i.imgur.com/HYnCvVt.png)
//TODO
## Technical Challenge
### Spark Tuning
//TODO
### HBase Bulk Load
//TODO
## Future Plan
//TODO
## Others
//TODO


