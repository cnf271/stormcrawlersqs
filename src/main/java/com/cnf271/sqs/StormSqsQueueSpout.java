package com.cnf271.sqs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.digitalpebble.stormcrawler.Constants;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.StringTabScheme;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by naweenf
 */
public class StormSqsQueueSpout extends BaseRichSpout {

    public static final Logger LOG = LoggerFactory.getLogger(StormSqsQueueSpout.class);
    private SpoutOutputCollector collector;
    private Queue<String> _inputFiles;
    private BufferedReader currentBuffer;
    private Scheme _scheme = new StringTabScheme();
    private LinkedList<byte[]> buffer = new LinkedList<>();
    private boolean active;
    private boolean withDiscoveredStatus;
    private int sleepTime;
    protected AmazonSQSAsync sqs;
    protected LinkedBlockingQueue<Message> queue;

    //AWS SQS Credentials
    private final String queueUrl;
    private final String sqsAccessKey;
    private final String sqsSecretAccessKey;
    private final String sqsRegion;

    public StormSqsQueueSpout(Properties properties, boolean withDiscoveredStatus) {
        this.queueUrl = properties.getProperty("aws.sqs.followerQueueUrl");
        this.withDiscoveredStatus = withDiscoveredStatus;
        this.sleepTime = 100;
        this.sqsAccessKey = properties.getProperty("aws.sqs.accessKey");
        this.sqsSecretAccessKey = properties.getProperty("aws.sqs.secretAccessKey");
        this.sqsRegion = properties.getProperty("aws.sqs.region");
    }

    public void setScheme(Scheme scheme) {
        _scheme = scheme;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;

        queue = new LinkedBlockingQueue<>();

        AWSCredentials credentials = new BasicAWSCredentials(sqsAccessKey,sqsSecretAccessKey);

        sqs = AmazonSQSAsyncClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(sqsRegion)
                .build();
    }

    @Override
    public void nextTuple() {

        if (queue.isEmpty()) {
            ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(
                    new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(10));
            queue.addAll(receiveMessageResult.getMessages());
        }

        Message message = queue.poll();
        if (message != null) {
            try {
                JSONParser parser = new JSONParser();
                JSONObject jsonObj = (JSONObject) parser.parse(message.getBody());
                List<Object> fields = this._scheme.deserialize(ByteBuffer.wrap(jsonObj.get("url").toString().getBytes()));

                if (withDiscoveredStatus) {
                    fields.add(Status.DISCOVERED);
                    this.collector.emit(Constants.StatusStreamName, fields, fields
                            .get(0).toString());
                    //Delete From SQS Queue
                    sqs.deleteMessageAsync(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
                } else {
                    sqs.deleteMessageAsync(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
                    this.collector.emit(fields, fields.get(0).toString());
                }
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        } else {
            // Still empty, go to sleep.
            Utils.sleep(sleepTime);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_scheme.getOutputFields());
        if (withDiscoveredStatus) {
            // add status field to output
            List<String> s = _scheme.getOutputFields().toList();
            s.add("status");
            declarer.declareStream(Constants.StatusStreamName, new Fields(s));
        }
    }

    @Override
    public void close() {
        sqs.shutdown();
        // Works around a known bug in the Async clients
        // @see https://forums.aws.amazon.com/thread.jspa?messageID=305371
        ((AmazonSQSAsyncClient) sqs).getExecutorService().shutdownNow();
    }

    @Override
    public void activate() {
        super.activate();
        active = true;
    }

    @Override
    public void deactivate() {
        super.deactivate();
        active = false;
    }


}
