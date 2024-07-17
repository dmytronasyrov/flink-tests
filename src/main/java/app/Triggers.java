package app;

import app.shoppingcart.ShoppingCartEvent;
import app.shoppingcart.ShoppingCartEventsGenerator;
import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

public class Triggers {

  private static final Instant baseInstant = Instant.parse("2022-02-15T00:00:00.000Z");
  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

  public static void main(String[] args) throws Exception {
    countTrigger();
    purgingTrigger();

    env.execute();
  }

  private static void purgingTrigger() {
    final SingleOutputStreamOperator<String> process = env.addSource(new ShoppingCartEventsGenerator(500, 2, baseInstant))
      .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
      .trigger(PurgingTrigger.of(CountTrigger.<TimeWindow>of(5)))
      .process(new CountByWindowAll());
    process.print();
  }

  private static void countTrigger() {
    final SingleOutputStreamOperator<String> process = env
      .addSource(new ShoppingCartEventsGenerator(500, 2, baseInstant))
      .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
      .trigger(CountTrigger.<TimeWindow>of(5))
      .process(new CountByWindowAll());
    process.print();
  }

  public static final class CountByWindowAll extends ProcessAllWindowFunction<ShoppingCartEvent, String, TimeWindow> {
    @Override
    public void process(final ProcessAllWindowFunction<ShoppingCartEvent, String, TimeWindow>.Context context, final Iterable<ShoppingCartEvent> iterable, final Collector<String> collector) throws Exception {
      final TimeWindow window = context.window();
      final String output = String.format("Window [%d - %d]   %d", window.getStart(), window.getEnd(), Iterators.size(iterable.iterator()));
      collector.collect(output);
    }
  }
}
