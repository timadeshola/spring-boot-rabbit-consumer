package com.cyberstarsng.springrabbitmqconsumer;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
@EnableRabbit
public class SpringRabbitmqConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringRabbitmqConsumerApplication.class, args);
    }

}


@Component
@RequiredArgsConstructor
@Slf4j
class Consumer {

    private final ProcessConsumer processConsumer;
    private final ObjectMapper mapper;

    @RabbitListener(queues = "rabbit.queue")
    public void consumerMessage(String data) throws JsonProcessingException {
        log.info("About to consumer data");
        Record record = mapper.readValue(data, Record.class);
        processConsumer.saveRecord(record);
        log.info("data consumed successfully {}", data);
    }
}

@Service
@RequiredArgsConstructor
class ProcessConsumer {

    private final RecordService recordService;

    public Record saveRecord(Record request) {
        return recordService.saveRecord(request);
    }

}

@Service
class RecordService {

    public static final List<Record> records = new ArrayList<>();

    public Record saveRecord(Record record) {
        records.add(record);
        return record;
    }
}

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
class Record implements Serializable {

    @JsonProperty("id")
    private Long id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("gender")
    private String gender;

    @JsonProperty("email")
    private String email;

}

@RestController
class RecordResource {

    @GetMapping("fetchRecords")
    public ResponseEntity<List<Record>> fetchRecords() {
        List<Record> response = RecordService.records;
        return ResponseEntity.ok(response);
    }
}
