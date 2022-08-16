package com.redhat.consulting.kafka;
import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class Consumer {
		
	public static void main(String args[]) {
		
		System.out.println("### Consuming");
		
		
		Properties props = buildProducerConfig();	
		KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(props);
		Pattern pattern = Pattern.compile(".*-audit");
		
		consumer.subscribe(pattern);
		
		ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
		
		try{
			  while (true){
			      records = consumer.poll(Duration.ofMillis(1000));
			      for (ConsumerRecord<String, String> record : records) {
			       
			    	  System.out.println(String.format("topic = %s,  value = %s",
			            
			          record.topic(), record.value()));
			      }
			  }
			  
			} finally {
			  consumer.close();
			}
		
	}
	
	
    private static Properties buildProducerConfig() {

        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"poc-kafka-bootstrap-amqstreams-kafka.apps.cluster-68zt2.68zt2.sandbox1906.opentlc.com:443");
        
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");                
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        p.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        
        p.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required  username=\"admin\" password=\"hKMBMikZHEII\";");
        
        p.setProperty("ssl.truststore.password","password");
        p.setProperty("ssl.truststore.location", "/opt/kafka/cert/truststore.jks");
        p.setProperty("security.protocol","SASL_SSL");        
        p.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        p.setProperty("ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1");
        
        return p;        
                
    }
	
	

}
