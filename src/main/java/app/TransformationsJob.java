package app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class TransformationsJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final DataStreamSource<Integer> numbers = env.fromElements(1, 2, 3, 4, 5);

    System.out.println("Old parallelism -> " + env.getParallelism());
    env.setParallelism(4);
    System.out.println("New parallelism -> " + env.getParallelism());

    System.out.println("Map");
    final SingleOutputStreamOperator<Integer> map = numbers.map(x -> x * 2);
    map.print();

    System.out.println("FlatMap");
    final SingleOutputStreamOperator<Integer> flatMap = numbers.flatMap(new FlatMapFunction<Integer, Integer>() {
      @Override
      public void flatMap(final Integer x, final Collector<Integer> xs) throws Exception {
        xs.collect(x);
        xs.collect(x + 1);
      }
    }).setParallelism(2);
    flatMap.print();

    System.out.println("Filter");
    final SingleOutputStreamOperator<Integer> filter = numbers.filter(x -> x != 3).setParallelism(6);
    filter.print();

    final SingleOutputStreamOperator<Integer> process = numbers.process(new ProcessFunction<Integer, Integer>() {
      @Override
      public void processElement(final Integer number, final ProcessFunction<Integer, Integer>.Context context, final Collector<Integer> collector) throws Exception {
        for (int i = 0; i < number; i++) {
          collector.collect(number * i * 10);
        }
      }
    });
    process.print();

    final KeyedStream<Integer, Boolean> integerBooleanKeyedStream = numbers.keyBy(x -> x % 2 == 0);
    final SingleOutputStreamOperator<Integer> reduce = integerBooleanKeyedStream.reduce(new ReduceFunction<Integer>() {
      @Override
      public Integer reduce(final Integer x, final Integer acc) throws Exception {
        return acc + x;
      }
    });
    reduce.print();

    System.out.println("File");
    numbers.writeAsText("output/result", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

    env.execute();
  }
}
