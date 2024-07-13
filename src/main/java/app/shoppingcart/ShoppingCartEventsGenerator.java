package app.shoppingcart;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ShoppingCartEventsGenerator implements SourceFunction<ShoppingCartEvent> {
  private final Integer sleepMsPerEvent;
  private final Integer batchSize;
  private final Instant baseInstant;
  private boolean running = true;
  private List<String> users = List.of("sasha", "masha", "dasha", "pasha", "vasja");

  public ShoppingCartEventsGenerator(Integer sleepMsPerEvent, Integer batchSize, Instant baseInstant) {
    this.sleepMsPerEvent = sleepMsPerEvent;
    this.batchSize = batchSize;
    this.baseInstant = baseInstant;
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

  private String getRandomUser() {
    return users.get(random(0, users.size() - 1));
  }

  private int random(int min, int max) {
    return (int) ((Math.random() * (max - min)) + min);
  }

  private int getRandomQuantity() {
    return random(0, 10);
  }
}
