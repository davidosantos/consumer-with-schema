package com.davidosantos.kafka.training.kafkatraining;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

@SpringBootApplication
@EnableScheduling
public class KafkaTrainingApplication {

	Logger logger = Logger.getLogger(this.toString());

	Properties props = new Properties();
	KafkaConsumer<String,DataRecordAvro> consumer;

	public static void main(String[] args) {

		SpringApplication.run(KafkaTrainingApplication.class, args);
	}

	@PostConstruct
	void doingSetups(){
		logger.info("Doing setups..");
		this.props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:19092,kafka-2:29092,kafka-3:39092");
		this.props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		this.props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		this.props.put("schema.registry.url", "http://schema-registry:8081");
        this.props.put("specific.avro.reader", "true");
		this.props.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer-1");

		consumer = new KafkaConsumer<>(props);


		consumer.subscribe(Arrays.asList("Java-Training3"), new ConsumerRebalanceListener() {
			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				logger.warning("Partition Assigned to me: " + partitions.toString());
				
			}
			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				logger.warning("Partition Revoked from me: " + partitions.toString());
				
			}
		});


	}

	@Scheduled(fixedRate = 2000l)
	void updateKafka(){
		//logger.info("Consuming messages..");
		ConsumerRecords<String, DataRecordAvro> messages = this.consumer.poll(Duration.ofMillis(800));

		messages.forEach(message -> {
				this.logger.info("Key: " + message.key());
				this.logger.info("Value: " + message.value());
				this.logger.info("offset: " + message.offset());
				this.logger.info("partition: " + message.partition());
				
				
		});
		
	}



}
