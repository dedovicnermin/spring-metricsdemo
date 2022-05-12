package io.example.metricsdemo;

import io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor;
import io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;

import java.util.Collections;
import java.util.Map;

@SpringBootApplication
@Slf4j
public class MetricsdemoApplication {
	static final String BOOTSTRAP = "localhost:9092";
	static final String TOPIC = "test-topic";
	static final String GROUP = "metricsdemo-consumer-group";

	public static void main(String[] args) {
		SpringApplication.run(MetricsdemoApplication.class, args);
	}

	@Bean
	public ConsumerFactory<?, ?> consumerFactory() {
		final Map<String, Object> configs = Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
				ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, Collections.singletonList(MonitoringConsumerInterceptor.class),
				ConsumerConfig.GROUP_ID_CONFIG, GROUP,
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				"confluent.monitoring.interceptor.bootstrap.servers", BOOTSTRAP
		);
		return new DefaultKafkaConsumerFactory<>(configs);
	}

	@Bean
	public ProducerFactory<?, ?> producerFactory() {
		final Map<String, Object> configs = Map.of(
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
				ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Collections.singletonList(MonitoringProducerInterceptor.class),
				ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
				ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
				"confluent.monitoring.interceptor.bootstrap.servers", BOOTSTRAP
		);
		return new DefaultKafkaProducerFactory<>(configs);
	}

	@Bean
	public NewTopic testTopic() {
		return TopicBuilder
				.name(TOPIC)
				.partitions(1)
				.replicas(1)
				.build();
	}

	@KafkaListener(topics = TOPIC)
	public void consume(final String recordValue) {
		log.info(recordValue);
	}

	@Bean
	public ApplicationRunner runner(final KafkaTemplate<String, String> producerTemplate) {
		return args -> {
			int i = 0;
			while (i <= 10000) {
				producerTemplate.send(TOPIC,"M"+i);
				Thread.sleep(600);
				i++;
			}
		};
	}

}
