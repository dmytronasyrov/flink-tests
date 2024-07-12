package app;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EssentialStreams {

  private final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  private final DataStream<Integer> simpleNumberStream = env.fromElements(1, 2, 3, 4);

  public void execute() throws Exception {
    simpleNumberStream.print();

    env.execute();
  }

  public static void main(String[] args) throws Exception {
    final EssentialStreams essentialStreams = new EssentialStreams();
    essentialStreams.execute();
  }
}
