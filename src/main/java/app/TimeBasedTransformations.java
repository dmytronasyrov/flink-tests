package app;

import app.shoppingcart.ShoppingCartEvent;
import app.shoppingcart.ShoppingCartEventsGenerator;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

public class TimeBasedTransformations {

  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  private static final Instant baseInstant = Instant.parse("2022-02-15T00:00:00.000Z");
  private static final ShoppingCartEventsGenerator shoppingCartEvents = new ShoppingCartEventsGenerator(100, 5, baseInstant);
  private final static DataStreamSource<ShoppingCartEvent> eventsStream = env.addSource(shoppingCartEvents);

  public static void main(String[] args) throws Exception {
    eventTimeTumblingWindow();
    processingTimeTumblingWindow();
    watermarkGeneratorTumblingWindow();


    env.execute();
  }

  private static void watermarkGeneratorTumblingWindow() {
    env.getConfig().setAutoWatermarkInterval(1000L);

    final WatermarkStrategy<ShoppingCartEvent> strategy = WatermarkStrategy
      .<ShoppingCartEvent>forGenerator(supplier -> new BoundedOutOfOrdernessGenerator(500L))
      .withTimestampAssigner(new TimestampAssigner());

    final SingleOutputStreamOperator<String> process = eventsStream
      .assignTimestampsAndWatermarks(strategy)
      .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(3)))
      .process(new CountByWindowAll());
    process.print();
  }

  private static void processingTimeTumblingWindow() {
    final SingleOutputStreamOperator<String> process = eventsStream.windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(3)))
      .process(new CountByWindowAll());
    process.print();
  }

  private static void eventTimeTumblingWindow() {
    final WatermarkStrategy<ShoppingCartEvent> strategy = WatermarkStrategy
      .<ShoppingCartEvent>forBoundedOutOfOrderness(Duration.ofMillis(500))
      .withTimestampAssigner(new TimestampAssigner());

    final SingleOutputStreamOperator<String> process = eventsStream.assignTimestampsAndWatermarks(strategy)
      .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(3)))
      .process(new CountByWindowAll());
    process.print();
  }

  public static final class TimestampAssigner implements SerializableTimestampAssigner<ShoppingCartEvent> {
    @Override
    public long extractTimestamp(final ShoppingCartEvent shoppingCartEvent, final long l) {
      return shoppingCartEvent.getTime().toEpochMilli();
    }
  }

  public static final class CountByWindowAll extends ProcessAllWindowFunction<ShoppingCartEvent, String, TimeWindow> {
    @Override
    public void process(final ProcessAllWindowFunction<ShoppingCartEvent, String, TimeWindow>.Context context, final Iterable<ShoppingCartEvent> iterable, final Collector<String> collector) throws Exception {
      final TimeWindow window = context.window();
      final String output = String.format("Window [%d - %d]   %d", window.getStart(), window.getEnd(), Iterators.size(iterable.iterator()));
      collector.collect(output);
    }
  }

  public static final class BoundedOutOfOrdernessGenerator implements WatermarkGenerator<ShoppingCartEvent> {

    private Long currentMaxTimestamp = 0L;
    private final Long maxDelay;

    public BoundedOutOfOrdernessGenerator(final Long maxDelay) {
      this.maxDelay = maxDelay;
    }

    @Override
    public void onEvent(final ShoppingCartEvent shoppingCartEvent, final long l, final WatermarkOutput watermarkOutput) {
      currentMaxTimestamp = Math.max(currentMaxTimestamp, shoppingCartEvent.getTime().toEpochMilli());
//      watermarkOutput.emitWatermark(new Watermark(shoppingCartEvent.getTime().toEpochMilli()));
    }

    @Override
    public void onPeriodicEmit(final WatermarkOutput watermarkOutput) {
      watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxDelay - 1));
    }
  }
}
