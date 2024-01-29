package com.kolban.beam;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.json.JSONObject;

public class MQPipe {
//  private static String IBM_MQ_HOSTNAME = "localhost";
  private static String IBM_MQ_HOSTNAME = "34.42.249.210";
  private static int IBM_MQ_PORT = 1414;
  private static String IBM_MQ_CHANNEL_NAME = "DEV.APP.SVRCONN";
  private static String IBM_MQ_QUEUE_MANAGER_NAME = "QM1";
  private static String IBM_MQ_QUEUE_NAME = "Q1";
  // The format of a Google Cloud PubSub topic must be:
  // projects/<project_id>/topics/<topic_name>
  private static String PUBLISH_TOPIC = "projects/test1-305123/topics/from_df";

  private static String IBM_MQ_USERID = "app";
  private static String IBM_MQ_PASSWORD = "passw0rd";


  /**
   * A DoFn that handles JmsRecord instances.
   */
  static class DoFn_ProcessJMSRecord extends DoFn<JmsRecord, String> {
    @ProcessElement
    public void processElement(
      @Element JmsRecord input,
      OutputReceiver<String> out) {
      // We will assume our incoming JMS Messages are text and contain data in the format:
      // SequenceNumber,DateString,Text
      String parts[] = input.getPayload().split(",");
      String sequenceNumber = parts[0];
      String nowString = parts[1];
      String text = parts[2];

      //
      // {
      //   "seq": <INT>,
      //   "now": <Date>
      //   "text": <String>
      // }
      /*
{
  "seq": 1234,
  "now": "2024-01-29T11:22:33",
  "text": "Hello world"
}
       */
      JSONObject jObj = new JSONObject().put("seq", sequenceNumber).put("now", nowString).put("text", text);

      System.out.println("Processed a message: seqNum: " + sequenceNumber);
      out.output(jObj.toString());
    }
  } // DoFn_ProcessJMSRecord


  public static void main(String[] args) {

    //RssListenerDataflowOptions rssListenerDataflowOptions =
    //  PipelineOptionsFactory.fromArgs(args).withValidation().as(RssListenerDataflowOptions.class);

    //rssListenerDataflowOptions.setStreaming(true);

    System.out.println("MQPipe beam pipeline starting ...");
    JmsConnectionFactory connectionFactory = null;

    try {
      System.out.println("... creating JMS Connection factory");
      JmsFactoryFactory jmsFactoryFactory = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);

      connectionFactory = jmsFactoryFactory.createConnectionFactory();
      connectionFactory.setStringProperty(WMQConstants.WMQ_HOST_NAME, IBM_MQ_HOSTNAME);
      connectionFactory.setIntProperty(WMQConstants.WMQ_PORT, IBM_MQ_PORT);
      connectionFactory.setStringProperty(WMQConstants.WMQ_CHANNEL, IBM_MQ_CHANNEL_NAME);
      connectionFactory.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
      connectionFactory.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, IBM_MQ_QUEUE_MANAGER_NAME);
      connectionFactory.setStringProperty(WMQConstants.USERID, IBM_MQ_USERID);
      connectionFactory.setStringProperty(WMQConstants.PASSWORD, IBM_MQ_PASSWORD);

    } catch (Exception e) {
      e.printStackTrace();
      //rssLogger.error("Exception in creating connection factory", e);
      throw new RuntimeException(e);
    }
    run(args, connectionFactory);

  }

  public static void run(String args[], JmsConnectionFactory connectionFactory) {
    System.out.println("... starting run of Pipeline");
    PipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().create();
    //PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

    // The plan is to use JmsIO.read() which will return a PCollection<JmsRecord>
    // For each JmsRecord, we'll log it to logging
    // We'll then transform it to something that can be written to PubSub
    // We'll then write it to PubSub
    // We'll see what gets lost under stress


    //read from ibm mq
    pipeline.apply("Read queue as JMS Record",
        JmsIO.read()
          .withConnectionFactory(connectionFactory)
          .withQueue(IBM_MQ_QUEUE_NAME))
      .apply(ParDo.of(new DoFn_ProcessJMSRecord()))
      .apply(PubsubIO.writeStrings().to(PUBLISH_TOPIC));

    System.out.println("About to run pipeline ...");
    pipeline.run();
  }
}
