package com.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;


public class DataAccessVerticle extends AbstractVerticle {

    private MongoClient mongoClient;
    private static final String DB_NAME = "staj-proje";
    private static final String COLLECTION_NAME = "feed";
    private static final String SUBS_COLLECTION = "subs";




    @Override
    public void start(Promise<Void> startPromise){

        startMongoClient();
        startHttpServer(startPromise);


    }

    private void startMongoClient() {
        JsonObject mongoConfig = new JsonObject()
                .put("db_name", DB_NAME)
                .put("connection_string", "mongodb://localhost:27017");
        mongoClient = MongoClient.createShared(vertx, mongoConfig);
    }

    private void startHttpServer(Promise<Void> startPromise) {

        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);

        router.get("/").handler(this::subscriptionsHandler);
        router.get("/:symbol").handler(this::dataBySymbolHandler);

        server.requestHandler(router);
        server.listen(8000, res -> {
            if (res.succeeded()) {
                System.out.println("HTTP Server started on port 8000");
            }else {
                System.out.println("HTTP Server start failed" +  res.cause().getMessage());
            }
        });

    }

    private void dataBySymbolHandler(RoutingContext ctx) {
        String symbol = ctx.pathParam("symbol");
        JsonObject query = new JsonObject().put("symbol", symbol);
        JsonObject sort = new JsonObject().put("timestamp", -1);
        FindOptions options = new FindOptions().setSort(sort);
        if (!SubscribedSymbols.getInstance().containsSymbol(symbol)) {
            ctx.response().setStatusCode(404).putHeader("content-type", "text-plain").end("Symbol not subscribed");
        }
        mongoClient.findWithOptions(COLLECTION_NAME, query, options, res -> {
            if (res.succeeded()) {
                JsonObject latest = res.result().getFirst();
                ctx.response().putHeader("content-type", "application/json").end(latest.encodePrettily());
            }else{
                ctx.response().setStatusCode(500).end("Failed to retrieve subscriptions");
            }});

    }

    private void subscriptionsHandler(RoutingContext ctx) {

            mongoClient.find(SUBS_COLLECTION, new JsonObject(), res -> {
                if (res.succeeded()) {
                    ctx.response().putHeader("content-type", "application/json")
                            .end(new JsonArray(res.result()).encodePrettily());
                }else{
                    ctx.response().setStatusCode(500).end("Failed to get subscriptions");
                }
            });

    }


}