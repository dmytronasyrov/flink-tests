package app.shoppingcart;

import java.time.Instant;
import java.util.UUID;

public class CatalogEventsGenerator extends EventGenerator<CatalogEvent> {
  public CatalogEventsGenerator(Integer sleepMillisBetweenEvents, Instant time, Long extraDelayInMillisOnEveryTenEvents) {
    super(sleepMillisBetweenEvents, time, id -> {
      return new ProductDetailsViewed(
        ShoppingCartEventsGenerator.getRandomUser(),
        time.plusSeconds(id),
        UUID.randomUUID().toString()
      );
    }, extraDelayInMillisOnEveryTenEvents);
  }

  public CatalogEventsGenerator() {
    super();
  }
}
