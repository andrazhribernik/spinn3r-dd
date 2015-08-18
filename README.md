# spinn3r-dd
spinn3r-dd detects all duplicates in Spinn3r dataset and stores unique items in specified directory. 

## Types of duplicates
There are different types of duplicates. Spinn3r documents might be the same in terms of content, title, url and host name. Not all combinations are relavant. This scripts generates following categorites of duplicates. 

* C - items with unique content
* HC - items with unique host name & content (if two items have same content and different host name are not duplicate in regards with this definition)
* HT -  items with unique host name & title
* HTC -  items with unique host name & title & content
* TC -  items with unique title & content
* U -  items with unique URL
* UC -  items with unique URL & content
* UT -  items with unique URL & title
* UTC -  items with unique URL & title & content

## Usage
To run the spinn3r-dd use `hadoop jar <path to the JAR> <main class packagename> -libjars <path to the spinn3r news-search jar> -Dmapred.reduce.tasks=<number of reducers> <input spinn3r directory> <output directory>`. 

For example:
`hadoop jar GenerateListOfOriginals-0.0.1-SNAPSHOT.jar edu.stanford.snap.spinn3r.dd.GenerateList -libjars Spinn3rHadoop-0.0.1-SNAPSHOT.jar -Dmapred.reduce.tasks=250 /dataset/spinn3r/web/web-*-*.txt /dataset/spinn3r/duplicates/web`

