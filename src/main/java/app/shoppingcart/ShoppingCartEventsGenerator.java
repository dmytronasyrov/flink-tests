package app.shoppingcart;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ShoppingCartEventsGenerator implements SourceFunction<ShoppingCartEvent> {
  private Integer sleepMsPerEvent;
  private Integer batchSize;
  private Instant baseInstant;
  private boolean running = true;
  private static List<String> users = List.of("sasha", "masha", "dasha", "pasha", "vasja");

  public ShoppingCartEventsGenerator(Integer sleepMsPerEvent, Integer batchSize, Instant baseInstant) {
    this.sleepMsPerEvent = sleepMsPerEvent;
    this.batchSize = batchSize;
    this.baseInstant = baseInstant;
  }

  public ShoppingCartEventsGenerator() {
  }

  @Override
  public void run(final SourceContext<ShoppingCartEvent> sourceContext) throws Exception {
    run(0L, sourceContext);
  }

  @Override
  public void cancel() {
    running = false;
  }

  private void run(Long startId, SourceFunction.SourceContext<ShoppingCartEvent> context) {
    if (running) {
      generateRandomEvents(startId).forEach(context::collect);
      try {
        Thread.sleep(batchSize * sleepMsPerEvent);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      run(startId + batchSize, context);
    }
  }

  private List<AddToShoppingCartEvent> generateRandomEvents(Long id) {
    final ArrayList<AddToShoppingCartEvent> addToShoppingCartEvents = new ArrayList<>();

    for (int i = 0; i < batchSize; i++) {
      final AddToShoppingCartEvent addToShoppingCartEvent = new AddToShoppingCartEvent(
        UUID.randomUUID().toString(),
        baseInstant.plusSeconds(id),
        getRandomUser(),
        getRandomQuantity()
      );
      addToShoppingCartEvents.add(addToShoppingCartEvent);
    }

    return addToShoppingCartEvents;
  }

  public static String getRandomUser() {
    return users.get(random(0, users.size() - 1));
  }

  private static int random(int min, int max) {
    return (int) ((Math.random() * (max - min)) + min);
  }

  private static int getRandomQuantity() {
    return random(0, 10);
  }

  public Integer getSleepMsPerEvent() {
    return sleepMsPerEvent;
  }

  public void setSleepMsPerEvent(final Integer sleepMsPerEvent) {
    this.sleepMsPerEvent = sleepMsPerEvent;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(final Integer batchSize) {
    this.batchSize = batchSize;
  }

  public Instant getBaseInstant() {
    return baseInstant;
  }

  public void setBaseInstant(final Instant baseInstant) {
    this.baseInstant = baseInstant;
  }

  public boolean isRunning() {
    return running;
  }

  public void setRunning(final boolean running) {
    this.running = running;
  }

  public static List<String> getUsers() {
    return users;
  }

  public static void setUsers(final List<String> users) {
    ShoppingCartEventsGenerator.users = users;
  }
}
