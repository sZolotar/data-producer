package com.example.dataproducer.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@ToString(callSuper = true)
@JsonPropertyOrder({"timestamp", "unit", "sensoridentifier", "samplingRate", "status", "value_nominal"})
public class SensorDataDto {

    private Long timestamp;
    private String unit;
    private String sensorIdentifier;
    private Integer samplingRate;
    private String status;
    private Double valueNominal;

    @JsonCreator
    public SensorDataDto(
            @JsonProperty("timestamp") Long timestamp,
            @JsonProperty("unit") String unit,
            @JsonProperty("sensoridentifier") String sensorIdentifier,
            @JsonProperty("samplingRate") Integer samplingRate,
            @JsonProperty("status") String status,
            @JsonProperty("value_nominal") Double valueNominal) {
        this.timestamp = timestamp;
        this.unit = unit;
        this.sensorIdentifier = sensorIdentifier;
        this.samplingRate = samplingRate;
        this.status = status;
        this.valueNominal = valueNominal;
    }

    public boolean IsCelsius(){
        return this.unit.equals("C");
    }

}
