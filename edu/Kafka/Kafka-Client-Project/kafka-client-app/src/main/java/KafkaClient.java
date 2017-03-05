import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import lombok.Data;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

/**
 * @author krunalsabnis@gmail.com
 *
 */
@Data
public class KafkaClient {
  private KafkaConsumer<String, String> consumer;
  private Producer<String, String> producer;
  private Logger log;

  /**
   * 
   * @param props
   */
  public KafkaClient(Properties props) {
    this.consumer = new KafkaConsumer<String, String>(props);
    this.producer = null;
    this.log = Logger.getLogger(Run.class);
    BasicConfigurator.configure();

  }

  /**
   * 
   * @return
   */
  public Map<String, List<PartitionInfo>> getTopicList() {
    Map<String, List<PartitionInfo>> topics;

    topics = consumer.listTopics();
    return topics;
  }

  /**
   * 
   * @param props
   * @return
   */
  public Producer<String, String> createKafkaProducer(Properties props) {
    if (producer == null) {
      this.producer = new KafkaProducer<String, String>(props);
    } else {
      log.info("kafka producer already created.");
    }
    return this.producer;
  }

  /**
   * .
   * @param props
   * @param topicName
   * @return
   */
  public void kafkaConsumer(Properties props, String topicName) {
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

    // Kafka Consumer subscribes list of topics here.
    consumer.subscribe(Arrays.asList(topicName));

    // print the topic name
    log.info("Subscribed to topic " + topicName);

    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(100);
        for (ConsumerRecord<String, String> record : records)
          // print the offset,key and value for the consumer records.
          log.info("offset = " + record.offset() + " key = "+  record.key() + "value = "+ record.value());
      }
    } catch (Exception e) {
      log.error("error while reading" + e.getMessage());
    } finally {
      consumer.close();
    }
    consumer.close();
    
  }
}
