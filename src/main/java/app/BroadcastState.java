package app;

import app.shoppingcart.AddToShoppingCartEvent;
import app.shoppingcart.ShoppingCartEvent;
import app.shoppingcart.SingleShoppingCardEventsGenerator;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.List;

public class BroadcastState {

  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  private static final Instant baseInstant = Instant.parse("2022-02-15T00:00:00.000Z");
  private static final SingleShoppingCardEventsGenerator<ShoppingCartEvent> stream1 = new SingleShoppingCardEventsGenerator<>(300, baseInstant, 0L, "kafka-1", true);
  private static final DataStreamSource<ShoppingCartEvent> source = env.addSource(stream1, TypeInformation.of(ShoppingCartEvent.class));

  public static void main(String[] args) throws Exception {
    final DataStreamSource<Integer> thresholds = env.addSource(new SourceFunction<Integer>() {
      @Override
      public void run(final SourceContext<Integer> sourceContext) throws Exception {
        List.of(2, 0, 4, 5, 6, 3).forEach(threshold -> {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }

          sourceContext.collect(threshold);
        });
      }

      @Override
      public void cancel() {
      }
    });

    final MapStateDescriptor<String, Integer> stateDescriptor = new MapStateDescriptor<>("thresholds", String.class, Integer.class);
    final BroadcastStream<Integer> broadcast = thresholds.broadcast(stateDescriptor);

    final KeyedStream<ShoppingCartEvent, String> userStream = source.keyBy(ShoppingCartEvent::getUserId);
    final SingleOutputStreamOperator<String> process = userStream.connect(broadcast)
      .process(new KeyedBroadcastProcessFunction<String, ShoppingCartEvent, Integer, String>() {
        final MapStateDescriptor<String, Integer> thresholdDescriptor = new MapStateDescriptor<>("thresholds", String.class, Integer.class);

        @Override
        public void processElement(final ShoppingCartEvent shoppingCartEvent, final KeyedBroadcastProcessFunction<String, ShoppingCartEvent, Integer, String>.ReadOnlyContext readOnlyContext, final Collector<String> collector) throws Exception {
          if (shoppingCartEvent instanceof AddToShoppingCartEvent) {
            Integer currentThreshold = readOnlyContext.getBroadcastState(thresholdDescriptor).get("quantity-threshold");
            if (currentThreshold == null) currentThreshold = 0;

            final Integer quantity = shoppingCartEvent.getQuantity();

            if (quantity > currentThreshold) {
              final String output = String.format("User %s attempting to purchase %d items of %s when threshold is %d", shoppingCartEvent.getUserId(), quantity, shoppingCartEvent.getSku(), currentThreshold);
              collector.collect(output);
            }
          }
        }

        @Override
        public void processBroadcastElement(final Integer newThreshold, final KeyedBroadcastProcessFunction<String, ShoppingCartEvent, Integer, String>.Context context, final Collector<String> collector) throws Exception {
          System.out.println("Threshold about to be changed -- " + newThreshold);

          final org.apache.flink.api.common.state.BroadcastState<String, Integer> stateThreshold = context.getBroadcastState(thresholdDescriptor);
          stateThreshold.put("quantity-threshold", newThreshold);
        }
      });
    process.print();

    env.execute();
  }
}
