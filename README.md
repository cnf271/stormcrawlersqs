# StormcrawlerSQS
 StormCrawler with SQS
 
 ![ ](https://github.com/cnf271/stormcrawlertest/blob/master/src/main/resources/article-2-image.png)
 
 [Medium Article](medium.com/@cnf271/running-stormcrawler-continuously-in-local-mode-without-a-storm-cluster-22e1aef72198)
 
 Run two instances separately using following commands.
 
 # Injector
 
  `java -cp target\stormcrawlersqs-1.0-SNAPSHOT.jar com.cnf271.InjectorTopology -conf es-conf.yaml -local`
 
 # Crawler
 
  `java -cp target\stormcrawlersqs-1.0-SNAPSHOT.jar com.cnf271.CrawlerTopology -conf es-conf.yaml -local`


 # Using ExecutorService to run a single instance with injector and crawler parallelly 

  `java -cp target\stormcrawlersqs-1.0-SNAPSHOT.jar com.cnf271.StormCrawlerApp`

 In order to work with SQS, add AWS credentials to config.properties file.
