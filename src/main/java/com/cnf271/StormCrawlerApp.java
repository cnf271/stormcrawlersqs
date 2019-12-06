package com.cnf271;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by naweenf
 */
public class StormCrawlerApp {

    public static void main(String[] args) {
        System.out.println(" ------ StormCrawlerApp Starting ------ ");
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(new InjectorTopology());
        executorService.execute(new CrawlerTopology());
    }

}
