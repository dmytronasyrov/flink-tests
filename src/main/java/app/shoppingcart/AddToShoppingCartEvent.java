package app.shoppingcart;

import java.time.Instant;

public class AddToShoppingCartEvent extends ShoppingCartEvent {
  final String sku;
  final Integer quantity;

  public AddToShoppingCartEvent(final String userId, final Instant instant, final String sku, final Integer quantity) {
    super(userId, instant);
    this.sku = sku;
    this.quantity = quantity;
  }

  public String getSku() {
    return sku;
  }

  public Integer getQuantity() {
    return quantity;
  }
}
