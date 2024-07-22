package app;

import app.shoppingcart.ShoppingCartEvent;
import app.shoppingcart.SingleShoppingCardEventsGenerator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;

public class SideOutputs {

  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  private static final Instant baseInstant = Instant.parse("2022-02-15T00:00:00.000Z");
  private static final SingleShoppingCardEventsGenerator<ShoppingCartEvent> stream1 = new SingleShoppingCardEventsGenerator<>(300, baseInstant, 0L, "kafka-1", true);
  private static final DataStreamSource<ShoppingCartEvent> source = env.addSource(stream1, TypeInformation.of(ShoppingCartEvent.class));
  private static final OutputTag<ShoppingCartEvent> mashaTag = new OutputTag<>("mash-event", TypeInformation.of(ShoppingCartEvent.class));

  //////////////////////////////////////////////////

  public static void main(String[] args) throws Exception {
    final SingleOutputStreamOperator<ShoppingCartEvent> process = source.process(new MashaEventsFunction());
    final SideOutputDataStream<ShoppingCartEvent> mashaStream = process.getSideOutput(mashaTag);
    mashaStream.print();

    env.execute();
  }

  public static final class MashaEventsFunction extends ProcessFunction<ShoppingCartEvent, ShoppingCartEvent> {
    @Override
    public void processElement(final ShoppingCartEvent shoppingCartEvent, final ProcessFunction<ShoppingCartEvent, ShoppingCartEvent>.Context context, final Collector<ShoppingCartEvent> collector) throws Exception {
      if (shoppingCartEvent.getUserId().equals("masha")) {
        context.output(mashaTag, shoppingCartEvent);
      } else {
        collector.collect(shoppingCartEvent);
      }
    }
  }
}
