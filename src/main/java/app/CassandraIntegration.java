package app;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.annotations.Table;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.io.Serializable;

public class CassandraIntegration {

  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

  public static void main(String[] args) throws Exception {
    final DataStreamSource<Person> persons = env.fromElements(
      new Person("petja", 70, "kiev"),
      new Person("katja", 16, "kharkov"),
      new Person("masha", 25, "odessa"),
      new Person("bzhshzheshek", 45, "krakow")
    );

    final ClusterBuilder clusterBuilder = new ClusterBuilder() {
      @Override
      protected Cluster buildCluster(final Cluster.Builder builder) {
        return builder.addContactPoint("cassandra-data.ludo.ninja").build();
      }
    };

    CassandraSink.addSink(persons)
      .setClusterBuilder(clusterBuilder)
      .setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
      .build();

    env.execute();
  }

  @Table(keyspace = "test_flink", name = "persons")
  public static final class Person implements Serializable {
    private String name;
    private Integer age;
    private String city;

    public Person(final String name, final Integer age, final String city) {
      this.name = name;
      this.age = age;
      this.city = city;
    }

    public Person() {
    }

    public String getName() {
      return name;
    }

    public void setName(final String name) {
      this.name = name;
    }

    public Integer getAge() {
      return age;
    }

    public void setAge(final Integer age) {
      this.age = age;
    }

    public String getCity() {
      return city;
    }

    public void setCity(final String city) {
      this.city = city;
    }
  }
}
