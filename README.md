# stormcrawlersqs
 StormCrawler with SQS
 
 Run two instances separately using following commands.
 
 # Injector
 
  `java -cp target\stormcrawlersqs-1.0-SNAPSHOT.jar com.cnf271.InjectorTopology -conf es-conf.yaml -local`
 
 # Crawler
 
  `java -cp target\stormcrawlersqs-1.0-SNAPSHOT.jar com.cnf271.CrawlerTopology -conf es-conf.yaml -local`


 # Using ExecutorService to run a single instance with injector and crawler parallelly 

  `java -cp target\stormcrawlersqs-1.0-SNAPSHOT.jar com.cnf271.StormCrawlerApp`

 In order to work with SQS, add AWS credentials to config.properties file.
