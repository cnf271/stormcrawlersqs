package com.cnf271;

import com.digitalpebble.stormcrawler.ConfigurableTopology;
import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.bolt.FetcherBolt;
import com.digitalpebble.stormcrawler.bolt.JSoupParserBolt;
import com.digitalpebble.stormcrawler.bolt.SiteMapParserBolt;
import com.digitalpebble.stormcrawler.bolt.URLPartitionerBolt;
import com.digitalpebble.stormcrawler.elasticsearch.bolt.IndexerBolt;
import com.digitalpebble.stormcrawler.elasticsearch.metrics.StatusMetricsBolt;
import com.digitalpebble.stormcrawler.elasticsearch.persistence.AggregationSpout;
import com.digitalpebble.stormcrawler.elasticsearch.persistence.StatusUpdaterBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by naweenf
 */
public class CrawlerTopology extends ConfigurableTopology implements Runnable{

    public static void main(String[] args) {
        System.out.println(" ------ CrawlerTopology Starting ------ ");
        ConfigurableTopology.start(new CrawlerTopology(), args);
    }

    @Override
    protected int run(String[] args) {
        System.out.println(" ------ Crawler Topology Started ------ ");

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new AggregationSpout());

        builder.setBolt("partitioner", new URLPartitionerBolt()).shuffleGrouping("spout");

        builder.setBolt("fetch", new FetcherBolt()).fieldsGrouping("partitioner", new Fields("key"));

        builder.setBolt("sitemap", new SiteMapParserBolt()).localOrShuffleGrouping("fetch");

        builder.setBolt("parse", new JSoupParserBolt()).localOrShuffleGrouping("sitemap");

        builder.setBolt("index", new IndexerBolt()).localOrShuffleGrouping("parse");

        builder.setBolt("status_metrics", new StatusMetricsBolt()).shuffleGrouping("spout");

        Fields furl = new Fields("url");

        builder.setBolt("status", new StatusUpdaterBolt(),10)
                .fieldsGrouping("fetch", Constants.StatusStreamName, furl)
                .fieldsGrouping("sitemap", Constants.StatusStreamName, furl)
                .fieldsGrouping("parse", Constants.StatusStreamName, furl)
                .fieldsGrouping("index", Constants.StatusStreamName, furl);

        return submit("ESCrawlerInstance", conf, builder);

    }

    @Override
    public void run() {
        String[] args = new String[]{"-conf","es-conf.yaml","-local"};
        ConfigurableTopology.start(new CrawlerTopology(), args);
    }
}
