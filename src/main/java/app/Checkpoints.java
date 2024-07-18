package app;

import app.shoppingcart.AddToShoppingCartEvent;
import app.shoppingcart.ShoppingCartEvent;
import app.shoppingcart.SingleShoppingCardEventsGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class Checkpoints {

  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  private static final Instant baseInstant = Instant.parse("2022-02-15T00:00:00.000Z");
  private static final SingleShoppingCardEventsGenerator<ShoppingCartEvent> stream1 = new SingleShoppingCardEventsGenerator<>(300, baseInstant, 0L, "kafka-1", true);
  private static final DataStreamSource<ShoppingCartEvent> source = env.addSource(stream1, TypeInformation.of(ShoppingCartEvent.class));

  public static void main(String[] args) throws Exception {
    env.getCheckpointConfig().setCheckpointInterval(5000);
    env.getCheckpointConfig().setCheckpointStorage("file:///Users/rudrafury/Projects/Ludo/github/tests/flink-playground/files");

    final SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = source.keyBy(ShoppingCartEvent::getUserId)
      .flatMap(new HighQuantityCheckpointedFunction(5L));
    flatMap.print();

    env.execute();
  }

  public static class HighQuantityCheckpointedFunction implements FlatMapFunction<ShoppingCartEvent, Tuple2<String, Long>>, CheckpointedFunction, CheckpointListener {

    private final Long threshold;
    private ValueState<Long> stateCount;

    public HighQuantityCheckpointedFunction(final Long threshold) {
      this.threshold = threshold;
    }

    @Override
    public void flatMap(final ShoppingCartEvent shoppingCartEvent, final Collector<Tuple2<String, Long>> collector) throws Exception {
      if (shoppingCartEvent instanceof AddToShoppingCartEvent) {
        if (shoppingCartEvent.getQuantity() > threshold) {
          if (stateCount.value() == null) {
            final long newState = 1L;
            stateCount.update(newState);

            collector.collect(Tuple2.of(shoppingCartEvent.getUserId(), newState));
          } else {
            final long newState = stateCount.value() + 1;
            stateCount.update(newState);

            collector.collect(Tuple2.of(shoppingCartEvent.getUserId(), newState));
          }
        }
      }
    }

    @Override
    public void initializeState(final FunctionInitializationContext functionInitializationContext) throws Exception {
      final ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("impossibleOrderCount", Long.class);
      stateCount = functionInitializationContext.getKeyedStateStore().getState(descriptor);
    }

    @Override
    public void snapshotState(final FunctionSnapshotContext functionSnapshotContext) throws Exception {
      System.out.println("CHECKPOINT AT " + functionSnapshotContext.getCheckpointTimestamp());
    }

    @Override
    public void notifyCheckpointAborted(final long checkpointId) throws Exception {
      CheckpointListener.super.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(final long l) throws Exception {
    }
  }
}
