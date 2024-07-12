package app;

import app.game.Player;
import app.game.PlayerRegistered;
import app.game.ServerEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

// --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED

public class WindowFunctions {

  // Constants

  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  private static final Instant serverStartTime = Instant.parse("2022-02-02T00:00:00.000Z");

  public static final Player petja = new Player("petja");
  public static final Player masha = new Player("masha");
  public static final Player sasha = new Player("sasha");
  public static final Player dima = new Player("dima");
  public static final Player nastia = new Player("nastia");
  public static final Player olja = new Player("olja");

  // Main

  public static void main(String[] args) throws Exception {
    final List<ServerEvent> events = eventsList();
    System.out.println("Events: " + events);

    final WatermarkStrategy<ServerEvent> strategy = WatermarkStrategy
      .<ServerEvent>forBoundedOutOfOrderness(Duration.ofMillis(500))
      .withTimestampAssigner(new SerializableTimestampAssigner<ServerEvent>() {
        @Override
        public long extractTimestamp(final ServerEvent serverEvent, final long timestamp) {
          return serverEvent.getEventTime().toEpochMilli();
        }
      });

    final SingleOutputStreamOperator<ServerEvent> serverEventSingleOutputStreamOperator = env
      .fromCollection(events, TypeInformation.of(ServerEvent.class))
      .assignTimestampsAndWatermarks(strategy);
    final TumblingEventTimeWindows tumblingEventTimeWindows = TumblingEventTimeWindows.of(Duration.ofSeconds(3));
    final AllWindowedStream<ServerEvent, TimeWindow> serverEventTimeWindowAllWindowedStream = serverEventSingleOutputStreamOperator
      .windowAll(tumblingEventTimeWindows);
    final SingleOutputStreamOperator<String> apply = serverEventTimeWindowAllWindowedStream.apply(new AllWindowFunction<ServerEvent, String, TimeWindow>() {
      @Override
      public void apply(final TimeWindow timeWindow, final Iterable<ServerEvent> input, final Collector<String> collector) throws Exception {
        int registeredEvents = 0;

        for (ServerEvent event : input) {
          if (event instanceof PlayerRegistered) {
            registeredEvents++;
          }
        }

        collector.collect(String.format("Window A [%d - %d] %d", timeWindow.getStart(), timeWindow.getEnd(), registeredEvents));
      }
    });
    apply.print();

    final SingleOutputStreamOperator<String> process = serverEventTimeWindowAllWindowedStream.process(new ProcessAllWindowFunction<ServerEvent, String, TimeWindow>() {
      @Override
      public void process(final ProcessAllWindowFunction<ServerEvent, String, TimeWindow>.Context context, final Iterable<ServerEvent> input, final Collector<String> collector) throws Exception {
        int registeredEvents = 0;
        final TimeWindow window = context.window();

        for (ServerEvent event : input) {
          if (event instanceof PlayerRegistered) {
            registeredEvents++;
          }
        }

        collector.collect(String.format("Window B [%d - %d] %d", window.getStart(), window.getEnd(), registeredEvents));
      }
    });
    process.print();

    final SingleOutputStreamOperator<Long> aggregate = serverEventTimeWindowAllWindowedStream.aggregate(new AggregateFunction<ServerEvent, Long, Long>() {
      @Override
      public Long createAccumulator() {
        return 0L;
      }

      @Override
      public Long add(final ServerEvent serverEvent, final Long acc) {
        if (serverEvent instanceof PlayerRegistered)
          return acc + 1L;
        else
          return acc;
      }

      @Override
      public Long getResult(final Long acc) {
        return acc;
      }

      @Override
      public Long merge(final Long acc2, final Long acc1) {
        return acc1 + acc2;
      }
    });
    aggregate.print();

    final KeyedStream<ServerEvent, String> serverEventStringKeyedStream = serverEventSingleOutputStreamOperator.keyBy(event -> event.getClass().getSimpleName());
    final SingleOutputStreamOperator<String> apply1 = serverEventStringKeyedStream.window(tumblingEventTimeWindows)
      .apply(new WindowFunction<ServerEvent, String, String, TimeWindow>() {
        @Override
        public void apply(final String key, final TimeWindow timeWindow, final Iterable<ServerEvent> input, final Collector<String> collector) throws Exception {
          final String output = String.format("AAAA Key: %s, Window: %s, %d", key, timeWindow, Iterators.size(input.iterator()));
          collector.collect(output);
        }
      });
    apply1.print();

    final SingleOutputStreamOperator<String> process1 = serverEventStringKeyedStream.window(tumblingEventTimeWindows)
      .process(new ProcessWindowFunction<ServerEvent, String, String, TimeWindow>() {
        @Override
        public void process(final String key, final ProcessWindowFunction<ServerEvent, String, String, TimeWindow>.Context context, final Iterable<ServerEvent> input, final Collector<String> collector) throws Exception {
          final String output = String.format("BBBB Key: %s, Window: %s, %d", key, context.window(), Iterators.size(input.iterator()));
          collector.collect(output);
        }
      });
    process1.print();

    env.execute();
  }

  // Private

  private static List<ServerEvent> eventsList() {
    return List.of(
      petja.register(serverStartTime, Duration.ofSeconds(2)),
      petja.online(serverStartTime, Duration.ofSeconds(2)),
      masha.register(serverStartTime, Duration.ofSeconds(3)),
      masha.online(serverStartTime, Duration.ofSeconds(4)),
      sasha.register(serverStartTime, Duration.ofSeconds(4)),
      dima.register(serverStartTime, Duration.ofSeconds(4)),
      nastia.register(serverStartTime, Duration.ofSeconds(6)),
      nastia.online(serverStartTime, Duration.ofSeconds(6)),
      olja.register(serverStartTime, Duration.ofSeconds(8)),
      sasha.online(serverStartTime, Duration.ofSeconds(10)),
      dima.online(serverStartTime, Duration.ofSeconds(10)),
      olja.online(serverStartTime, Duration.ofSeconds(10))
    );
  }
}
