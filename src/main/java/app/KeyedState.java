package app;

import app.shoppingcart.ShoppingCartEvent;
import app.shoppingcart.ShoppingCartEventsGenerator;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class KeyedState {

  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

  private static final Instant baseInstant = Instant.parse("2022-02-15T00:00:00.000Z");
  private static final ShoppingCartEventsGenerator shoppingCartEvents = new ShoppingCartEventsGenerator(1000, 1, baseInstant);
  private static final DataStreamSource<ShoppingCartEvent> eventsStream = env.addSource(shoppingCartEvents);
  private static final KeyedStream<ShoppingCartEvent, String> userEvents = eventsStream.keyBy(ShoppingCartEvent::getSku);

  public static void main(String[] args) throws Exception {
//    valueState();
//    listState();
//    mapState();
    clearState();

    env.execute();
  }

  private static void clearState() {
    final SingleOutputStreamOperator<String> process = userEvents.process(new KeyedProcessFunction<String, ShoppingCartEvent, String>() {
      ListState<ShoppingCartEvent> state;

      @Override
      public void open(final OpenContext openContext) throws Exception {
        super.open(openContext);

        final ListStateDescriptor<ShoppingCartEvent> descriptor = new ListStateDescriptor<>("shopping-cart-events", ShoppingCartEvent.class);
        final StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofSeconds(20))
          .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
          .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
          .build();
        descriptor.enableTimeToLive(ttl);
        state = getRuntimeContext().getListState(descriptor);
      }

      @Override
      public void processElement(final ShoppingCartEvent shoppingCartEvent, final KeyedProcessFunction<String, ShoppingCartEvent, String>.Context context, final Collector<String> collector) throws Exception {
        state.add(shoppingCartEvent);

        final int numOfElements = Iterators.size(state.get().iterator());

        if (numOfElements > 10)
          state.clear();

        final String collectedEvents = StreamSupport.stream(Spliterators.spliteratorUnknownSize(state.get().iterator(), Spliterator.ORDERED), false)
          .map(Object::toString)
          .collect(Collectors.joining(", "));
        final String output = String.format("User %s - [%s]", shoppingCartEvent.getUserId(), collectedEvents);
        collector.collect(output);
      }
    });
    process.print();
  }

  private static void mapState() {
    final SingleOutputStreamOperator<String> process = userEvents.process(new KeyedProcessFunction<String, ShoppingCartEvent, String>() {
      MapState<String, Long> state;

      @Override
      public void open(final OpenContext openContext) throws Exception {
        super.open(openContext);

        final MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("per-type-counter", String.class, Long.class);
        state = getRuntimeContext().getMapState(descriptor);
      }

      @Override
      public void processElement(final ShoppingCartEvent shoppingCartEvent, final KeyedProcessFunction<String, ShoppingCartEvent, String>.Context context, final Collector<String> collector) throws Exception {
        final String eventType = shoppingCartEvent.getClass().getSimpleName();

        if (state.contains(eventType)) {
          final Long oldCount = state.get(eventType);
          state.put(eventType, oldCount + 1);
        } else {
          state.put(eventType, 1L);
        }

        final String collectedEvents = StreamSupport.stream(Spliterators.spliteratorUnknownSize(state.entries().iterator(), Spliterator.ORDERED), false)
          .map(Object::toString)
          .collect(Collectors.joining(", "));
        final String output = String.format("%s - %s", context.getCurrentKey(), collectedEvents);
        collector.collect(output);
      }
    });
    process.print();
  }

  private static void listState() {
    final SingleOutputStreamOperator<String> process = userEvents.process(new KeyedProcessFunction<String, ShoppingCartEvent, String>() {

      ListState<ShoppingCartEvent> stateEvents;

      @Override
      public void open(final OpenContext openContext) throws Exception {
        super.open(openContext);

        final ListStateDescriptor<ShoppingCartEvent> descriptor = new ListStateDescriptor<>("shopping-cart-events", ShoppingCartEvent.class);
        stateEvents = getRuntimeContext().getListState(descriptor);
      }

      @Override
      public void processElement(final ShoppingCartEvent shoppingCartEvent, final KeyedProcessFunction<String, ShoppingCartEvent, String>.Context context, final Collector<String> collector) throws Exception {
        stateEvents.add(shoppingCartEvent);

        final Iterable<ShoppingCartEvent> stateEventsIter = stateEvents.get();
        final String collectedEvents = StreamSupport.stream(Spliterators.spliteratorUnknownSize(stateEventsIter.iterator(), Spliterator.ORDERED), false)
          .map(Object::toString)
          .collect(Collectors.joining(", "));
        final String output = String.format("User %s - [%s]", shoppingCartEvent.getUserId(), collectedEvents);
        collector.collect(output);
      }
    });
    process.print();
  }

  private static void valueState() {
    final SingleOutputStreamOperator<String> process = userEvents.process(new KeyedProcessFunction<String, ShoppingCartEvent, String>() {
      private ValueState<Long> stateCounter;

      @Override
      public void open(final OpenContext openContext) throws Exception {
        super.open(openContext);

        final ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("events-counter", Long.class);
        stateCounter = getRuntimeContext().getState(descriptor);
      }

      @Override
      public void processElement(final ShoppingCartEvent shoppingCartEvent, final KeyedProcessFunction<String, ShoppingCartEvent, String>.Context context, final Collector<String> collector) throws Exception {
        final Long eventsCounter = stateCounter.value();

        if (eventsCounter == null) {
          final long quantity = shoppingCartEvent.getQuantity().longValue();
          stateCounter.update(quantity);
          final String output = String.format("User %s - %d", shoppingCartEvent.getUserId(), quantity);
          collector.collect(output);
        } else {
          final long quantity = shoppingCartEvent.getQuantity().longValue();
          final long newValue = eventsCounter + quantity;
          stateCounter.update(newValue);
          final String output = String.format("User %s - %d", shoppingCartEvent.getUserId(), newValue);
          collector.collect(output);
        }
      }
    });
    process.print();
  }
}
