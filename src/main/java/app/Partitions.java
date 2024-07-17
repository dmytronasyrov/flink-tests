package app;

import app.shoppingcart.ShoppingCartEvent;
import app.shoppingcart.SingleShoppingCardEventsGenerator;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;

public class Partitions {

  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  private static final Instant baseInstant = Instant.now();

  public static void main(String[] args) throws Exception {
    final Partitioner<String> partitioner = new Partitioner<>() {
      @Override
      public int partition(final String key, final int numOfPartitions) {
        final String output = String.format("Number of max partitions: %d", numOfPartitions);
        System.out.println(output);
        return key.hashCode() % numOfPartitions;
      }
    };

    final SingleShoppingCardEventsGenerator<ShoppingCartEvent> stream1 = new SingleShoppingCardEventsGenerator<>(300, baseInstant, 0L, "kafka-1", true);
    final DataStream<ShoppingCartEvent> shoppingCartEventDataStream = env
      .addSource(stream1, TypeInformation.of(ShoppingCartEvent.class))
      .partitionCustom(partitioner, ShoppingCartEvent::getUserId);
    shoppingCartEventDataStream.print();

    env.execute();
  }
}
