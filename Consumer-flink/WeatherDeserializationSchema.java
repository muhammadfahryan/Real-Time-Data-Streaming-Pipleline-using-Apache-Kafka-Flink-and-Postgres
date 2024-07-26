package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeatherDeserializationSchema extends AbstractDeserializationSchema<Weather> {

   private static final long serialVersionUID = 1L;
   private static final Logger LOG = LoggerFactory.getLogger(WeatherDeserializationSchema.class);

   private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = JsonMapper.builder().build().registerModule(new JavaTimeModule());
    }

    @Override
    public Weather deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, Weather.class);
    }
}
