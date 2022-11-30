package org.acme;

import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.runtime.ReactiveTransactional;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.mutiny.core.Vertx;

import javax.inject.Inject;
import javax.transaction.NotSupportedException;

import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import javax.transaction.Transactional;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import org.hibernate.reactive.mutiny.Mutiny;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.util.function.Consumer;
import java.util.function.Function;

@Path("/fruit")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class FruitResource {

  @Inject
  Vertx vertx;

  @Inject
  FruitRepository fruitRepository;

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public Uni<String> hello() {
    return Uni.createFrom().item("Hello from RESTEasy Reactive");
  }

  @POST
  @Path("{failTask2}/{failMainTask}")
  public Uni<Fruit> create(Fruit fruit, boolean failTask2, boolean failMainTask) throws SystemException {
    return task1(fruit, failTask2, failMainTask).onFailure().call(x -> undoTask2().invoke(y -> {
      System.out.println(y+" due to "+x.getMessage());
    }));
  }

  // @Transactional
  @ReactiveTransactional
  Uni<Fruit> task1(Fruit fruit,boolean failTask2,boolean failMainTask) throws IllegalStateException, SystemException {
    return Panache.getSession().flatMap(session -> {
      return session.withTransaction(tx -> {
        if (failMainTask) {
          tx.markForRollback();
        }
        // this should roll back when there is a failure 
        return fruitRepository.persist(fruit).onItem().call(x->task2(failTask2));
      };)
    };)
  }

  private Uni<String> task2(boolean fail) {
    return Uni.createFrom().item("done task2").map(Unchecked.function(x -> {
      if (fail)
        throw new Exception("non-transactional task failed");
      else {
        return x;
      }
    }));

  }

  private Uni<String> undoTask2() {
    return Uni.createFrom().item("undone task2");
  }

}