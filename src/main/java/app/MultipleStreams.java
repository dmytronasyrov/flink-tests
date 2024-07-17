package app;

import app.shoppingcart.SingleShoppingCardEventsGenerator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;

import app.shoppingcart.ShoppingCartEvent;

public class MultipleStreams {

  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  private static final Instant baseInstant = Instant.now();

  public static void main(String[] args) throws Exception {
    final SingleShoppingCardEventsGenerator<ShoppingCartEvent> stream1 = new SingleShoppingCardEventsGenerator<>(300, baseInstant, 0L, "kafka-1", true);
    final SingleShoppingCardEventsGenerator<ShoppingCartEvent> stream2 = new SingleShoppingCardEventsGenerator<>(1000, baseInstant, 0L, "kafka-2", true);

    final DataStreamSource<ShoppingCartEvent> source1 = env.addSource(stream1, TypeInformation.of(ShoppingCartEvent.class));
    final DataStreamSource<ShoppingCartEvent> source2 = env.addSource(stream2, TypeInformation.of(ShoppingCartEvent.class));
    final DataStream<ShoppingCartEvent> union = source1.union(source2);
    union.print();

    env.execute();
  }
}
