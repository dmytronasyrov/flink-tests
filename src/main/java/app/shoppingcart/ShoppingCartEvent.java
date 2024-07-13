package app.shoppingcart;

import java.time.Instant;

public abstract class ShoppingCartEvent {
  final String userId;
  final Instant time;

  public ShoppingCartEvent(final String userId, final Instant time) {
    this.userId = userId;
    this.time = time;
  }

  public String getUserId() {
    return userId;
  }

  public Instant getTime() {
    return time;
  }
}
