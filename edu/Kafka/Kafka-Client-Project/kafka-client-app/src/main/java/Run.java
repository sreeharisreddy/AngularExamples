import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;


public class Run {
	private static final String TOPIC_NAME = "DBS_TOPIC";
	private static final String KAFKA_BROKER = "54.149.41.179:9092";
	private static final long eventsCount = 30;
	final static Logger log = Logger.getLogger(Run.class.getName());

	public static void main(String[] args) {
	  
	  if("k".equals(args[0])) {
	    
	    
	    
	    
	    return;
	  }
	  
	  
		Logger log = Logger.getLogger(Run.class);
		BasicConfigurator.configure();
        //PropertyConfigurator.configure("log4j.properties");
		Properties props = new Properties();
		if (args.length == 0 ) {
		  help();
		  return;
		} else 	if (args.length > 1) {
			props.put("bootstrap.servers", args[1]);
      props.put("zookeeper.connect", args[1]);
		} else {
			props.put("bootstrap.servers", KAFKA_BROKER);
			props.put("zookeeper.connect", KAFKA_BROKER);
		}
		
        props.put("session.timeout.ms", "30000");

		props.put("group.id", "dbs-consumer-group");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaClient client = new KafkaClient(props);
		
		
		switch (args[0]) {
		case "listTopics" :
			
			for (String topicName: client.getTopicList().keySet()) {
				log.info("Topic Name "+ topicName);
			}
			break;
		case "mockProducer" :
			log.info("sending dummy data");
			LogSimulator logSimulator = new LogSimulator();
			logSimulator.getRandomLog();
			
			Producer<String, String> producer = client.createKafkaProducer(getProducerProperty());
			for (long nEvents = 0; nEvents < eventsCount; nEvents++) { 
	            String ip = logSimulator.getRandomIP();
	            String msg = logSimulator.getRandomMessage(ip);
	            ProducerRecord<String, String> producerRecord =
	            		new ProducerRecord<String, String>(TOPIC_NAME, ip, msg);
	            producer.send(producerRecord);
	            log.info((nEvents + " event sent"));
	            }
			log.info("completed");	
	        producer.close();
			break;
		case "mockConsumer" :
		  Properties prop = new Properties();
		  prop.put("bootstrap.servers", KAFKA_BROKER);
		  prop.put("group.id", "test");
		  prop.put("enable.auto.commit", "true");
		  prop.put("auto.commit.interval.ms", "1000");
		  prop.put("session.timeout.ms", "30000");
		  prop.put("key.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
		  prop.put("value.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
		  client.kafkaConsumer(props, TOPIC_NAME);
			break;
		}       
	}
	
	
	
	private static Properties getProducerProperty() {
		
	    // create instance for properties to access producer configs   
	    Properties props = new Properties();
	     
	    //Assign localhost id
	    props.put("bootstrap.servers", KAFKA_BROKER);
	      
	    //Set acknowledgements for producer requests.
	    props.put("acks", "1");
	      
	    //If the request fails, the producer can automatically retry,
	    props.put("retries", 0);
	      
	    //Specify buffer size in config
	    props.put("batch.size", 16384);
	     
	    //Reduce the no of requests less than 0   
	    props.put("linger.ms", 1);
	      
	    //The buffer.memory controls the total amount of memory available to the producer for buffering.   
	    props.put("buffer.memory", 33554432);
	      
	    props.put("key.serializer", 
	       "org.apache.kafka.common.serialization.StringSerializer");
	         
	    props.put("value.serializer", 
	       "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	private static void help() {
	  System.out.println("argument required : [listTopics | mockProducer | mockConsumer]");
	}
}
