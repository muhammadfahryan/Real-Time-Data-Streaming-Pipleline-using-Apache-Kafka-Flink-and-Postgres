package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.TopicIdPartition;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.api.common.functions.MapFunction;

import javax.xml.crypto.Data;
import java.sql.PreparedStatement;

public class Main {

    static final String bootstrapservers = "localhost:9092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Weather> source = KafkaSource.<Weather>builder()
                .setBootstrapServers(bootstrapservers)
                .setProperty("partition-discovery.interval-millis", "1000")
                .setTopics("weathers")
                .setGroupId("weathers-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new WeatherDeserializationSchema())
                .build();

        DataStreamSource<Weather> kafka = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka");

        System.out.println("kafka source created");

        DataStream<Tuple2<MyAverage, Double>> averageTemperatureStream = kafka.keyBy(myEvent -> myEvent.city)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                .aggregate(new AverageAggregator());

        DataStream<Tuple2<MyAverage, Double>> cityAndValueStream = averageTemperatureStream.map(new MapFunction<Tuple2<MyAverage, Double>, Tuple2<MyAverage, Double>>() {
            @Override
            public Tuple2<MyAverage, Double> map(Tuple2<MyAverage, Double> input) throws Exception {
                return new Tuple2<>(input.f0, input.f1);
            }
        });

        System.out.println("aggregation created");

        // Menambahkan sink ke PostgreSQL
        cityAndValueStream.addSink(JdbcSink.sink(
                "INSERT INTO public.weather_averages (city, average_temperature) VALUES (?, ?)",
                (statement, event) -> {
                    statement.setString(1, event.f0.city);
                    statement.setDouble(2, event.f1);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://117.54.18.59:5432/postgres")
                        .withDriverName("org.postgresql.Driver")
                        .withUsername("postgres")
                        .withPassword("cauW68QP7ywgxDR2=")
                        .build()
        ));

        cityAndValueStream.print();

        env.execute("Flink Kafka to PostgreSQL");
    }

    public static class AverageAggregator implements AggregateFunction<Weather, MyAverage, Tuple2<MyAverage,Double>> {

            @Override
                public MyAverage createAccumulator() {
                    return new MyAverage();
            }

            @Override
            public MyAverage add(Weather weather, MyAverage myAverage) {
                myAverage.city = weather.city;
                myAverage.count = myAverage.count + 1;
                myAverage.sum = myAverage.sum + weather.temperature;
                return myAverage;
            }

            @Override
            public Tuple2<MyAverage,Double> getResult(MyAverage myAverage) {
                return new Tuple2<>(myAverage, myAverage.sum / myAverage.count);
            }

            @Override
            public MyAverage merge(MyAverage myAverage, MyAverage acc1) {
                myAverage.sum = myAverage.sum + acc1.sum;
                myAverage.count = myAverage.count + acc1.count;
                return myAverage;
            }
        }

    public static class MyAverage {
        public String city;
        public Integer count = 0;
        public Double sum = 0d;

        @Override
        public String toString() {
            return "MyAverage{" +
                    "city='" + city + '\'' +
                    ", count=" + count +
                    ", sum=" + sum +
                    '}';
        }
    }

}
