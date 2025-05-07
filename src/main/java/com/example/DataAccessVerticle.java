package com.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;


public class DataAccessVerticle extends AbstractVerticle {

    private MongoClient mongoClient;
    private static final String DB_NAME = "staj-proje";
    private static final String COLLECTION_NAME = "feed";

    @Override
    public void start(Promise<Void> startPromise){


        JsonObject mongoConfig = new JsonObject()
                .put("db_name", DB_NAME)
                .put("connection_string", "mongodb://localhost:27017");
        mongoClient = MongoClient.createShared(vertx, mongoConfig);


        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);

        router.get("/").handler(req -> {
            mongoClient.find("subs", new JsonObject(), res -> {
                if (res.succeeded()) {
                    req.response().putHeader("content-type", "application/json")
                            .end(new JsonArray(res.result()).encodePrettily());
                }else{
                    req.response().setStatusCode(500).end("Failed to get subscriptions");
                }
            });





        });
        router.get("/:symbol").handler(req -> {
            String symbol = req.pathParam("symbol");
            JsonObject query = new JsonObject().put("symbol", symbol);
            JsonObject sort = new JsonObject().put("timestamp", -1);
            FindOptions options = new FindOptions().setSort(sort);
            mongoClient.findWithOptions(COLLECTION_NAME, query, options, res -> {
                if (res.succeeded()) {
                    JsonObject latest = res.result().getFirst();
                    req.response().putHeader("content-type", "application/json").end(latest.encodePrettily());
                }else{
                    req.response().setStatusCode(500).end("Failed to retrieve subscriptions");
                }});

        });




        server.requestHandler(router);
        server.listen(8000, res -> {
            if (res.succeeded()) {
                startPromise.complete();
                System.out.println("HTTP Server started on port 8000");
            }else {
                startPromise.fail(res.cause());
                System.out.println("HTTP Server start failed" +  res.cause().getMessage());
            }
        });




    }


}