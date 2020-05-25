package com.example.dataproducer.controller;

import com.example.dataproducer.service.CsvToKafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    private final CsvToKafkaService csvToKafkaService;

    @Autowired
    public TestController(CsvToKafkaService csvToKafkaService) {
        this.csvToKafkaService = csvToKafkaService;
    }

    @GetMapping("/sendData")
    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    public void sendData() {
        csvToKafkaService.sendCsvToKafka();
    }
}
