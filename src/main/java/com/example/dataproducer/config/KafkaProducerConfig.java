package com.example.dataproducer.config;

import com.example.dataproducer.model.SensorDataDto;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.example.dataproducer.util.Constants.TOPIC_INPUT_DATA_SOURCE;
import static com.example.dataproducer.util.Constants.TOPIC_OUTPUT_DATA_SOURCE;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.streams.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, SensorDataDto> producerFactory() throws ExecutionException, InterruptedException {

        Map<String, Object> configProps = new HashMap<>();

        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new JsonSerde<>().serializer().getClass());

        AdminClient adminClient = AdminClient.create(configProps);
        Set<String> topicList = adminClient.listTopics().names().get();

        if (!topicList.contains(TOPIC_INPUT_DATA_SOURCE) && !topicList.contains(TOPIC_OUTPUT_DATA_SOURCE))
            adminClient.createTopics(createTopics());

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, SensorDataDto> kafkaTemplate() throws ExecutionException, InterruptedException {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Initialization Topics
     */
    private List<NewTopic> createTopics() {
        List<NewTopic> newTopics = new ArrayList<>();

        NewTopic newInputDataTopic = new NewTopic(TOPIC_INPUT_DATA_SOURCE, 2, (short) 1);
        NewTopic newOutputDataTopic = new NewTopic(TOPIC_OUTPUT_DATA_SOURCE, 2, (short) 1);

        newTopics.add(newInputDataTopic);
        newTopics.add(newOutputDataTopic);

        return newTopics;
    }
}
