package app;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

// --add-opens=java.base/java.util=ALL-UNNAMED

public class FeezBuzz {

  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

  public static void main(String[] args) throws Exception {
    final DataStreamSource<Long> numbers = env.fromSequence(1, 100);
    final SingleOutputStreamOperator<FizzBuzzResult> results = numbers.map(x -> {
      if (x % 3 == 0 && x % 5 == 0) return new FizzBuzzResult(x, "feezbuzz");
      else if (x % 3 == 0) return new FizzBuzzResult(x, "feez");
      else if (x % 5 == 0) return new FizzBuzzResult(x, "buzz");
      else return new FizzBuzzResult(x, x.toString());
    });
    final SingleOutputStreamOperator<Long> feezbuzz = results
      .filter((FilterFunction<FizzBuzzResult>) fizzBuzzResult -> fizzBuzzResult.getOutput().equals("feezbuzz"))
      .map(FizzBuzzResult::getNumber);
//    feezbuzz.writeAsText("output/feezbuzz", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

    final StreamingFileSink<Long> sink = StreamingFileSink.<Long>forRowFormat(
        new Path("output/streaming_sink"),
        new SimpleStringEncoder<>("UTF-8")
      )
      .build();
    feezbuzz.addSink(sink).setParallelism(1);

    env.execute();
  }

  public static final class FizzBuzzResult {
    private final long number;
    private final String output;

    public FizzBuzzResult(final long number, final String output) {
      this.number = number;
      this.output = output;
    }

    public long getNumber() {
      return number;
    }

    public String getOutput() {
      return output;
    }
  }
}
