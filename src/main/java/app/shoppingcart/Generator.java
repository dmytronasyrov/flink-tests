package app.shoppingcart;

import java.io.Serializable;

@FunctionalInterface
public interface Generator<T, O> extends Serializable {
  O accept(T l);
}
