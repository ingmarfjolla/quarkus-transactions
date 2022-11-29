package org.acme;

import io.quarkus.hibernate.reactive.panache.common.runtime.ReactiveTransactional;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.mutiny.core.Vertx;

import javax.inject.Inject;
import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
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

    long blockTime = 500;

    @Inject
    Vertx vertx;

    @Inject
    FruitRepository fruitRepository;

    @Inject TransactionManager tm;

    @Inject
    Mutiny.Session session;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Uni<String> hello() {
        return Uni.createFrom().item("Hello from RESTEasy Reactive");
    }

    @POST
    public Uni<Fruit> create(Fruit fruit) throws SystemException {
        boolean failTask2=false;
        boolean failMainTask=false;
        Uni<Fruit> uni=task1(fruit,failTask2,failMainTask);
        int status=tm.getStatus();
        if (javax.transaction.Status.STATUS_COMMITTED==status) {
            return uni;
        } else {
            return uni.call(x->undoTask2());
        }
    }

    @ReactiveTransactional
    Uni<Fruit> task1(Fruit fruit,boolean failTask2,boolean failMainTask) throws IllegalStateException, SystemException {
        //At this point the get status returns 6 which is "status no transaction", and the get transaction is null.
        // I would have expected the get status to return status active
        Log.info(tm.getStatus());
        Log.info(tm.getTransaction());
        if (failMainTask){
            tm.setRollbackOnly();
//            tm.getTransaction().setRollbackOnly();
        }
        return fruitRepository.persist(fruit).
                // this should roll back when there is a failure
                        onItem().call(x->task2(failTask2));
    }

    private Uni<String> task2(boolean fail) {
        return Uni.createFrom().emitter(e -> vertx.setTimer(blockTime, x -> e.complete("completing...")))
                .map(Unchecked.function(x -> {
                    if (fail)
                        throw new Exception("non-transactional task failed");
                    else {
                        return x.toString();
                    }
                }));
    }

    private Uni<String> undoTask2() {
        return Uni.createFrom().item("undone task2");
    }

}