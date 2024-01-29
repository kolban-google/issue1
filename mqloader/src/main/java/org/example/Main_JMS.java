/**
 * This application uses IBM MQ and its JMS API to write messages to an MQ Queue.  The messages are written
 * in a loop with a target message rate.  The messages are JMS Text message with the format:
 * SequenceNumber,DateTime,Text
 */
package org.example;
// We can clear messages using
// runmqsc CLEAR QLOCAL(Q1)

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;

import javax.jms.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

// Write messages to the queue using JMS
// Sample is https://raw.githubusercontent.com/ibm-messaging/mq-dev-samples/master/gettingStarted/jms/com/ibm/mq/samples/jms/JmsPut.java
public class Main_JMS {

  // System exit status value (assume unset value to be 1)
  private static int status = 1;

  // Create variables for the connection to MQ
  private static final String HOST = "localhost"; // Host name or IP address
  private static final int PORT = 1414; // Listener port for your queue manager
  private static final String CHANNEL = "DEV.APP.SVRCONN"; // Channel name
  private static final String QMGR = "QM1"; // Queue manager name
  private static final String APP_USER = "app"; // User name that application uses to connect to MQ
  private static final String APP_PASSWORD = "passw0rd"; // Password that the application uses to connect to MQ
  private static final String QUEUE_NAME = "Q1"; // Queue that the application uses to put and get messages to and from

  private static final int MAX_MESSAGES=100000;

  public static void main(String[] args) {

    JMSContext context = null;
    Destination destination = null;
    JMSProducer producer = null;
    JMSConsumer consumer = null;
    long messagesSecond = 50;
    long delta = 1000 / messagesSecond;
    long lastPut = System.currentTimeMillis();
    long lastLog = lastPut;
    long batchCounter = 0; // The current batch of messages in the logging unit
    long sequenceNumber = 0; // The total number of messages put to the queue

    try {
      // Create a connection factory
      JmsFactoryFactory jmsFactoryFactory = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
      JmsConnectionFactory connectionFactory = jmsFactoryFactory.createConnectionFactory();

      // Set the properties
      connectionFactory.setStringProperty(WMQConstants.WMQ_HOST_NAME, HOST);
      connectionFactory.setIntProperty(WMQConstants.WMQ_PORT, PORT);
      connectionFactory.setStringProperty(WMQConstants.WMQ_CHANNEL, CHANNEL);
      connectionFactory.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
      connectionFactory.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, QMGR);
      connectionFactory.setStringProperty(WMQConstants.WMQ_APPLICATIONNAME, "JmsPutGet (JMS)");
      connectionFactory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
      connectionFactory.setStringProperty(WMQConstants.USERID, APP_USER);
      connectionFactory.setStringProperty(WMQConstants.PASSWORD, APP_PASSWORD);
      //cf.setStringProperty(WMQConstants.WMQ_SSL_CIPHER_SUITE, "*TLS12ORHIGHER");

      // Create JMS objects
      context = connectionFactory.createContext();
      destination = context.createQueue("queue:///" + QUEUE_NAME);
      producer = context.createProducer();

      while (sequenceNumber < MAX_MESSAGES) {
        sequenceNumber++;

        String nowString = ZonedDateTime.now( ZoneOffset.UTC ).format( DateTimeFormatter.ISO_INSTANT );
        // Example message
        // 123,2024-01-29T11:23:45,this is a JMS message
        TextMessage message = context.createTextMessage(sequenceNumber+","+nowString + ",this is a JMS message");

        producer.send(destination, message);
        batchCounter++;

        //System.out.println("Sent message:\n" + message);

        // We just put a message, now we need to perhaps sleep for a bit to achieve the
        // delay for the target message rate.
        long delay = System.currentTimeMillis() - lastPut;
        if (delay < delta) {
          Thread.sleep(delta - delay);
        }

        lastPut = System.currentTimeMillis();

        // Maybe we need to log the rate.
        if ((System.currentTimeMillis() - lastLog) > 1000) {
          System.out.println("Put " + batchCounter + " messages, total now written: " + sequenceNumber);
          batchCounter = 0;
          lastLog = lastPut;
        }
      } // While

      context.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  } // Main
} // Main_JMS