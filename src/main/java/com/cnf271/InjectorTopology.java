package com.cnf271;

import com.cnf271.sqs.StormSqsQueueSpout;
import com.cnf271.util.PropertyFileReader;
import com.digitalpebble.stormcrawler.ConfigurableTopology;
import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.bolt.URLFilterBolt;
import com.digitalpebble.stormcrawler.elasticsearch.persistence.StatusUpdaterBolt;
import com.digitalpebble.stormcrawler.util.URLStreamGrouping;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Properties;

/**
 * Created by naweenf
 */
public class InjectorTopology  extends ConfigurableTopology implements Runnable {


    public static void main(String[] args) {
        System.out.println(" ------ InjectorTopology Starting ------ ");
        ConfigurableTopology.start(new InjectorTopology(), args);
    }

    @Override
    protected int run(String[] args) {
        System.out.println(" ------ Injector Topology Started ------ ");

        TopologyBuilder builder = new TopologyBuilder();
        Properties properties = new PropertyFileReader().loadPropertiesFile("config.properties");
        builder.setSpout("spout", new StormSqsQueueSpout(properties, true));

        Fields key = new Fields("url");

        builder.setBolt("filter", new URLFilterBolt()).fieldsGrouping("spout",
                Constants.StatusStreamName, key);

        builder.setBolt("enqueue", new StatusUpdaterBolt(), 10).customGrouping(
                "filter", Constants.StatusStreamName, new URLStreamGrouping());

        return submit("ESInjectorInstance", conf, builder);
    }

    @Override
    public void run() {
        String[] args = new String[]{"-conf","es-conf.yaml","-local"};
        ConfigurableTopology.start(new InjectorTopology(), args);
    }
}
