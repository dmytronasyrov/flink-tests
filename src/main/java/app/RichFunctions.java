package app;

import app.shoppingcart.AddToShoppingCartEvent;
import app.shoppingcart.ShoppingCartEvent;
import app.shoppingcart.ShoppingCartEventsGenerator;
import app.shoppingcart.SingleShoppingCardEventsGenerator;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class RichFunctions {

  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  private static final Instant baseInstant = Instant.parse("2022-02-15T00:00:00.000Z");

  public static void main(String[] args) throws Exception {
    env.setParallelism(1);

//    richFunction();
//    processFunction();
//    purchasedItemsRichFunc();
    purchasedItemsProcessFunc();

    env.execute();
  }

  private static void purchasedItemsProcessFunc() {
    final SingleOutputStreamOperator<String> stream = env.addSource(new ShoppingCartEventsGenerator(500, 2, baseInstant))
      .filter(event -> event instanceof AddToShoppingCartEvent)
      .map(event -> (AddToShoppingCartEvent) event)
      .process(new ProcessFunction<AddToShoppingCartEvent, String>() {
        @Override
        public void processElement(final AddToShoppingCartEvent addToShoppingCartEvent, final ProcessFunction<AddToShoppingCartEvent, String>.Context context, final Collector<String> collector) throws Exception {
          for (int i = 0; i < addToShoppingCartEvent.getQuantity(); i++) {
            collector.collect(addToShoppingCartEvent.getSku());
          }
        }
      });
    stream.print();
  }

  private static void purchasedItemsRichFunc() {
    final SingleOutputStreamOperator<String> stream = env.addSource(new ShoppingCartEventsGenerator(500, 2, baseInstant))
      .filter(event -> event instanceof AddToShoppingCartEvent)
      .map(event -> (AddToShoppingCartEvent) event)
      .flatMap(new RichFlatMapFunction<AddToShoppingCartEvent, String>() {
        @Override
        public void open(final OpenContext openContext) throws Exception {
          super.open(openContext);
          System.out.println("Open function");
        }

        @Override
        public void close() throws Exception {
          super.close();
          System.out.println("Close function");
        }

        @Override
        public void flatMap(final AddToShoppingCartEvent addToShoppingCartEvent, final Collector<String> collector) throws Exception {
          for (int i = 0; i < addToShoppingCartEvent.getQuantity(); i++) {
            collector.collect(addToShoppingCartEvent.getSku());
          }
        }
      });
    stream.print();
  }

  private static void processFunction() {
    final DataStreamSource<Integer> numberStream = env.fromElements(1, 2, 3, 4, 5, 6);
    final SingleOutputStreamOperator<Integer> process = numberStream.process(new ProcessFunction<Integer, Integer>() {
      @Override
      public void open(final OpenContext openContext) throws Exception {
        super.open(openContext);
        System.out.println("Open function");
      }

      @Override
      public void close() throws Exception {
        super.close();
        System.out.println("Close function");
      }

      @Override
      public void processElement(final Integer value, final ProcessFunction<Integer, Integer>.Context context, final Collector<Integer> collector) throws Exception {
        collector.collect(value * 10);
      }
    });
    process.print();
  }

  private static void richFunction() {
    final DataStreamSource<Integer> numberStream = env.fromElements(1, 2, 3, 4, 5, 6);
    final SingleOutputStreamOperator<Integer> map = numberStream.map(new RichMapFunction<Integer, Integer>() {
      @Override
      public Integer map(final Integer val) throws Exception {
        return val * 10;
      }

      @Override
      public void close() throws Exception {
        super.close();
        System.out.println("Close function");
      }

      @Override
      public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("Open function");
      }
    });
    map.print();
  }
}
