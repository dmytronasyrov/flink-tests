package app.shoppingcart;

import java.time.Instant;

public class AddToShoppingCartEvent extends ShoppingCartEvent {
  public AddToShoppingCartEvent(final String userId, final Instant time, final String sku, final Integer quantity) {
    super(userId, time, sku, quantity);
  }

  public AddToShoppingCartEvent() {
    super();
  }
}
