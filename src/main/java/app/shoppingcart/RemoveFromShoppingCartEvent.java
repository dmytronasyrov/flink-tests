package app.shoppingcart;

import java.time.Instant;

public class RemoveFromShoppingCartEvent extends ShoppingCartEvent {
  public RemoveFromShoppingCartEvent(final String userId, final Instant time, final String sku, final Integer quantity) {
    super(userId, time, sku, quantity);
  }

  public RemoveFromShoppingCartEvent() {
  }
}
