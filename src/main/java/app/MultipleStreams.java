package app;

import app.shoppingcart.CatalogEvent;
import app.shoppingcart.CatalogEventsGenerator;
import app.shoppingcart.SingleShoppingCardEventsGenerator;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.time.Instant;

import app.shoppingcart.ShoppingCartEvent;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;

public class MultipleStreams {

  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  private static final Instant baseInstant = Instant.now();

  public static void main(String[] args) throws Exception {
//    union();
//    windowJoin();
//    intervalJoins();
    connect();

    env.execute();
  }

  private static void union() {
    final SingleShoppingCardEventsGenerator<ShoppingCartEvent> stream1 = new SingleShoppingCardEventsGenerator<>(300, baseInstant, 0L, "kafka-1", true);
    final SingleShoppingCardEventsGenerator<ShoppingCartEvent> stream2 = new SingleShoppingCardEventsGenerator<>(1000, baseInstant, 0L, "kafka-2", true);

    final DataStreamSource<ShoppingCartEvent> source1 = env.addSource(stream1, TypeInformation.of(ShoppingCartEvent.class));
    final DataStreamSource<ShoppingCartEvent> source2 = env.addSource(stream2, TypeInformation.of(ShoppingCartEvent.class));
    final DataStream<ShoppingCartEvent> union = source1.union(source2);
    union.print();
  }

  private static void windowJoin() {
    final SingleShoppingCardEventsGenerator<ShoppingCartEvent> stream1 = new SingleShoppingCardEventsGenerator<>(300, baseInstant, 0L, "kafka-1", true);
    final CatalogEventsGenerator stream2 = new CatalogEventsGenerator(200, baseInstant, 0L);

    final DataStreamSource<ShoppingCartEvent> source1 = env.addSource(stream1, TypeInformation.of(ShoppingCartEvent.class));
    final DataStreamSource<CatalogEvent> source2 = env.addSource(stream2, TypeInformation.of(CatalogEvent.class));

    final DataStream<String> apply = source1.join(source2)
      .where(ShoppingCartEvent::getUserId)
      .equalTo(CatalogEvent::getUserId)
      .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
      .apply(new FlatJoinFunction<ShoppingCartEvent, CatalogEvent, String>() {
        @Override
        public void join(final ShoppingCartEvent shoppingCartEvent, final CatalogEvent catalogEvent, final Collector<String> collector) throws Exception {
          final String output = String.format("User %s browsed at %s and bought at %s", shoppingCartEvent.getUserId(), catalogEvent.getTime(), shoppingCartEvent.getTime());
          collector.collect(output);
        }
      });
    apply.print();
  }

  private static void intervalJoins() {
    final SingleShoppingCardEventsGenerator<ShoppingCartEvent> stream1 = new SingleShoppingCardEventsGenerator<>(300, baseInstant, 0L, "kafka-1", true);
    final CatalogEventsGenerator stream2 = new CatalogEventsGenerator(200, baseInstant, 0L);

    final WatermarkStrategy<ShoppingCartEvent> strategyShoppingCartEvent = WatermarkStrategy.<ShoppingCartEvent>forBoundedOutOfOrderness(Duration.ofMillis(500))
      .withTimestampAssigner(new SerializableTimestampAssigner<ShoppingCartEvent>() {
        @Override
        public long extractTimestamp(final ShoppingCartEvent shoppingCartEvent, final long l) {
          return shoppingCartEvent.getTime().toEpochMilli();
        }
      });

    final WatermarkStrategy<CatalogEvent> strategyCatalogEvent = WatermarkStrategy.<CatalogEvent>forBoundedOutOfOrderness(Duration.ofMillis(500))
      .withTimestampAssigner(new SerializableTimestampAssigner<CatalogEvent>() {
        @Override
        public long extractTimestamp(final CatalogEvent catalogEvent, final long l) {
          return catalogEvent.getTime().toEpochMilli();
        }
      });

    final KeyedStream<ShoppingCartEvent, String> source1 = env.addSource(stream1, TypeInformation.of(ShoppingCartEvent.class))
      .assignTimestampsAndWatermarks(strategyShoppingCartEvent)
      .keyBy(ShoppingCartEvent::getUserId);

    final KeyedStream<CatalogEvent, String> source2 = env.addSource(stream2, TypeInformation.of(CatalogEvent.class))
      .assignTimestampsAndWatermarks(strategyCatalogEvent)
      .keyBy(CatalogEvent::getUserId);

    final SingleOutputStreamOperator<String> process = source1.intervalJoin(source2)
      .between(Duration.ofSeconds(-2), Duration.ofSeconds(2))
      .lowerBoundExclusive()
      .upperBoundExclusive()
      .process(new ProcessJoinFunction<ShoppingCartEvent, CatalogEvent, String>() {
        @Override
        public void processElement(final ShoppingCartEvent shoppingCartEvent, final CatalogEvent catalogEvent, final ProcessJoinFunction<ShoppingCartEvent, CatalogEvent, String>.Context context, final Collector<String> collector) throws Exception {
          final String output = String.format("User %s browsed at %s and bought at %s", shoppingCartEvent.getUserId(), catalogEvent.getTime(), shoppingCartEvent.getTime());
          collector.collect(output);
        }
      });
    process.print();
  }

  private static void connect() {
    env.setParallelism(1);
    env.setMaxParallelism(1);

    final SingleShoppingCardEventsGenerator<ShoppingCartEvent> stream1 = new SingleShoppingCardEventsGenerator<>(300, baseInstant, 0L, "kafka-1", true);
    final CatalogEventsGenerator stream2 = new CatalogEventsGenerator(200, baseInstant, 0L);

    final DataStreamSource<ShoppingCartEvent> source1 = env.addSource(stream1, TypeInformation.of(ShoppingCartEvent.class)).setParallelism(1);
    final DataStreamSource<CatalogEvent> source2 = env.addSource(stream2, TypeInformation.of(CatalogEvent.class)).setParallelism(1);

    final SingleOutputStreamOperator<Double> process = source1.connect(source2)
      .process(new CoProcessFunction<ShoppingCartEvent, CatalogEvent, Double>() {
        int shoppingCartEventCount = 0;
        int catalogEventCount = 0;

        @Override
        public void processElement1(final ShoppingCartEvent shoppingCartEvent, final CoProcessFunction<ShoppingCartEvent, CatalogEvent, Double>.Context context, final Collector<Double> collector) throws Exception {
          shoppingCartEventCount += 1;

          final double output = (shoppingCartEventCount * 100.0) / (shoppingCartEventCount + catalogEventCount);
          collector.collect(output);
        }

        @Override
        public void processElement2(final CatalogEvent catalogEvent, final CoProcessFunction<ShoppingCartEvent, CatalogEvent, Double>.Context context, final Collector<Double> collector) throws Exception {
          catalogEventCount += 1;

          final double output = (shoppingCartEventCount * 100.0) / (shoppingCartEventCount + catalogEventCount);
          collector.collect(output);
        }
      });
    process.print();
  }
}
