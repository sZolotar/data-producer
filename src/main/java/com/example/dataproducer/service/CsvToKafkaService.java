package com.example.dataproducer.service;

import com.example.dataproducer.model.SensorDataDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.example.dataproducer.util.Constants.TOPIC_INPUT_DATA_SOURCE;


@Service
@Slf4j
public class CsvToKafkaService {

    private static final String FILE_NAME = "sensor_data.csv";
    private final Pattern pattern = Pattern.compile(",");

    private final KafkaTemplate<String, SensorDataDto> kafkaTemplate;

    @Autowired
    public CsvToKafkaService(KafkaTemplate<String, SensorDataDto> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Send Data from a CSV file to kafka topic
     */
    public void sendCsvToKafka() {
        parseCsvFile()
                .forEach(sensorDataDto -> kafkaTemplate.send(TOPIC_INPUT_DATA_SOURCE, sensorDataDto));
    }

    /**
     * Read csv data file from resources
     * @return Outputting deserialization data from a CSV file
     */
    private List<SensorDataDto> parseCsvFile() {

        List<SensorDataDto> sensors = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(
                new InputStreamReader(
                        Objects.requireNonNull(getClass()
                                .getClassLoader()
                                .getResourceAsStream(FILE_NAME))))) {
            sensors = in
                    .lines()
                    .skip(1)
                    .map(line -> {
                        String[] x = pattern.split(line);
                        return SensorDataDto.builder()
                                .timestamp(Long.valueOf(x[0]))
                                .unit(x[1])
                                .sensorIdentifier(x[2])
                                .samplingRate(Integer.valueOf(x[3]))
                                .status(x[4])
                                .valueNominal(Double.valueOf(x[5]))
                                .build();
                    })
                    .collect(Collectors.toList());

        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();

        }

        log.info("Producer: [{}} matches in Celsius from [{}] rows",
                sensors.stream().filter(SensorDataDto::IsCelsius).count(),
                sensors.size());

        return sensors;
    }
}
