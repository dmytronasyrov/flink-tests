package app.shoppingcart;

import java.io.Serializable;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

public class SingleShoppingCardEventsGenerator<T extends ShoppingCartEvent> extends EventGenerator<T> implements Serializable {

  private String sourceId;
  private boolean generatedRemoved = false;

  public SingleShoppingCardEventsGenerator(final Integer sleepMillisBetweenEvents, final Instant baseInstant, final Long extraDelayInMillisOnEveryTenEvents, final String sourceId, final boolean generatedRemoved) {
    super(sleepMillisBetweenEvents, baseInstant, new ShoppingGenerator<>(baseInstant, generatedRemoved), extraDelayInMillisOnEveryTenEvents);
    this.sourceId = sourceId;
    this.generatedRemoved = generatedRemoved;
  }

  public SingleShoppingCardEventsGenerator() {
    super();
  }

  private static String skuGen(Long id) {
    if (id == null)
      return UUID.randomUUID().toString();
    else
      return String.format("%s_%s", id, UUID.randomUUID());
  }

  public static class ShoppingGenerator<T extends ShoppingCartEvent> implements Generator<Long, T> {

    private final Instant baseInstant;
    private final boolean generatedRemoved;
    private final Random random = new Random();

    public ShoppingGenerator(final Instant baseInstant, final boolean generatedRemoved) {
      this.baseInstant = baseInstant;
      this.generatedRemoved = generatedRemoved;
    }

    @Override
    public T accept(final Long id) {
      final String randomUser = ShoppingCartEventsGenerator.getRandomUser();
      final String sku = SingleShoppingCardEventsGenerator.skuGen(id);
      final int quantity = random.nextInt();
      final Instant time = baseInstant.plusSeconds(id);

      if (generatedRemoved && random.nextBoolean()) {
        return (T) new RemoveFromShoppingCartEvent(randomUser, time, sku, quantity);
      } else {
        return (T) new AddToShoppingCartEvent(randomUser, time, sku, quantity);
      }
    }
  }

  public String getSourceId() {
    return sourceId;
  }

  public void setSourceId(final String sourceId) {
    this.sourceId = sourceId;
  }

  public boolean isGeneratedRemoved() {
    return generatedRemoved;
  }

  public void setGeneratedRemoved(final boolean generatedRemoved) {
    this.generatedRemoved = generatedRemoved;
  }
}
