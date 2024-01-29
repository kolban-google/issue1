package org.example;
// We can clear messages using
// runmqsc CLEAR QLOCAL(Q1)
import com.ibm.mq.MQEnvironment;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.MQConstants;
import com.ibm.msg.client.wmq.WMQConstants;

public class Main {
  public static void main(String[] args) {
    long messagesSecond = 50;
    long delta = 1000 / messagesSecond;
    String msg = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n<KLog MajorVersion=\"6\" MinorVersion=\"5\" FixVersion=\"4\" xmlns=\"http://www.nrf-arts.org/IXRetail/namespace/\">\r\n    <Transaction TypeCode=\"00\">\r\n        <TransactionSourceApplicationTypeCode>ePOS</TransactionSourceApplicationTypeCode>\r\n        <BusinessUnit>\r\n            <UnitID TypeCode=\"RetailStore\">01400968</UnitID>\r\n        </BusinessUnit>\r\n        <WorkstationID>366</WorkstationID>\r\n        <SequenceNumber>942</SequenceNumber>\r\n        <POSLogDateTime>2024-01-22T15:42:57-05:00</POSLogDateTime>\r\n        <GrossPositive>5.25</GrossPositive>\r\n        <GrossNegative>0</GrossNegative>\r\n        <ReceiptDateTime>2024-01-22T15:42:57</ReceiptDateTime>\r\n        <OrderFulfillmentApplication>\r\n            <ApplicationName>Starbucks</ApplicationName>\r\n        </OrderFulfillmentApplication>\r\n        <ModalityType>INSTORE</ModalityType>\r\n        <FulfillmentMethod>INSTORETHIRDPARTY</FulfillmentMethod>\r\n        <RetailTransaction>\r\n            <LineItem>\r\n                <Sale ItemType=\"Stock\">\r\n                    <MerchandiseHierarchy Level=\"Department\">50</MerchandiseHierarchy>\r\n                    <POSIdentity POSIDType=\"00\">\r\n                        <POSItemID>00019640926197</POSItemID>\r\n                    </POSIdentity>\r\n                    <ItemFulfillmentSource>01400968</ItemFulfillmentSource>\r\n                    <UnitRetailPrice Currency=\"USD\">5.25</UnitRetailPrice>\r\n                    <ExtendedAmount Currency=\"USD\">5.25</ExtendedAmount>\r\n                    <Quantity UnitOfMeasureCode=\"EA\">1</Quantity>\r\n                </Sale>\r\n                <SequenceNumber>1</SequenceNumber>\r\n            </LineItem>\r\n            <LineItem>\r\n                <Tender TenderType=\"11\" Status=\"0\">\r\n                    <Amount Currency=\"USD\">5.25</Amount>\r\n                </Tender>\r\n                <SequenceNumber>1</SequenceNumber>\r\n            </LineItem>\r\n            <LineItem>\r\n                <Tax TaxType=\"PlanA\" TypeCode=\"Sale\">\r\n                    <Amount Currency=\"USD\">0.00</Amount>\r\n                </Tax>\r\n                <SequenceNumber>1</SequenceNumber>\r\n            </LineItem>\r\n        </RetailTransaction>\r\n    </Transaction>\r\n</KLog>";
    try {
      MQEnvironment.properties.put(CMQC.TRANSPORT_PROPERTY, CMQC.TRANSPORT_MQSERIES_CLIENT);
      MQEnvironment.channel = "DEV.APP.SVRCONN";
      MQEnvironment.userID = "app";
      MQEnvironment.password = "passw0rd";

      // Create a connection to the queue manager
      MQQueueManager qmgr = new MQQueueManager("QM1");

      // Create a queue object
      MQQueue queue = qmgr.accessQueue("Q1", MQConstants.MQOO_OUTPUT);
      long lastPut = System.currentTimeMillis();
      long lastLog = lastPut;
      long messageCounter = 0; // The current batch of messages in the logging unit
      long totalMessages = 0; // The total number of messages put to the queue


      while (totalMessages < 10000) {
        // Create a message object
        MQMessage message = new MQMessage();

        // Write a message to the queue
        message.writeUTF(msg);
        message.format = CMQC.MQFMT_STRING;
        queue.put(message);
        messageCounter++;
        totalMessages++;

        // We just put a message, now we need to perhaps sleep for a bit to achieve the
        // delay for the target message rate.
        long delay = System.currentTimeMillis() - lastPut;
        if (delay < delta) {
          Thread.sleep(delta - delay);
        }

        lastPut = System.currentTimeMillis();

        // Maybe we need to log the rate.
        if ((System.currentTimeMillis() - lastLog) > 1000) {
          System.out.println("Put " + messageCounter + " messages");
          messageCounter = 0;
          lastLog = lastPut;
        }
      }
      // Close the queue object
      queue.close();

      // Disconnect from the queue manager
      qmgr.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}