package com.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;

import java.util.HashSet;
import java.util.Set;


public class BinanceWebsocket extends AbstractVerticle {
    private static final String BINANCE_STREAM_TYPE = "aggTrade";
    private WebSocket webSocket;
    private HttpClient httpClient;
    private Set<String> subscribedSymbols = new HashSet<>();


    private static final String DB_NAME = "staj-proje";
    private static final String COLLECTION_NAME = "feed";
    private static final String SUBS_COLLECTION = "subs";
    private MongoClient mongoClient;


    @Override
    public void start(Promise<Void> startPromise) {

        JsonObject mongoConfig = new JsonObject()
                .put("db_name", DB_NAME)
                .put("connection_string", "mongodb://localhost:27017");

        mongoClient = MongoClient.createShared(vertx, mongoConfig);
        System.out.println("Connected to MongoDB");



        HttpClientOptions httpClientOptions = new HttpClientOptions()
                .setSsl(true)
                .setDefaultPort(9443)
                .setDefaultHost("stream.binance.com");
        this.httpClient = vertx.createHttpClient(httpClientOptions);


        mongoClient.find(SUBS_COLLECTION, new JsonObject(), res -> {
            if (res.succeeded()) {
                res.result().forEach(data -> subscribedSymbols.add(data.getString("symbol")));
                connectToBinance(startPromise);
            }else {
                startPromise.fail(res.cause());
            }
        });

        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);


        router.post("/subscribe/:symbol").handler(event -> {

            String symbol = event.pathParam("symbol").toLowerCase();

            if (symbol == null || symbol.isEmpty()) {
                System.out.println("Invalid symbol received");
                return;
            }
            JsonObject jsonSubs = new JsonObject()
                    .put("symbol", symbol);
            if(subscribedSymbols.contains(jsonSubs.getString("symbol"))) {
                event.response().setStatusCode(500).putHeader("content-type", "text-plain").end("Symbol already subscribed");
                System.out.println("Symbol already subscribed");
                return;
            }
            mongoClient.insert(SUBS_COLLECTION,jsonSubs,res ->{


                if (res.succeeded()) {
                    subscribedSymbols.add(symbol);
                    event.response().setStatusCode(200).end("Subscribed to " + symbol);
                    System.out.println("Subscription saved to MongoDB");
                }else {
                    event.response().setStatusCode(500).end("Failed to subscribe: " + res.cause().getMessage());
                    System.out.println("Failed to save subscription to MongoDB");
                }
            });
            int requestID = 1;
            String streamName = symbol.toLowerCase() + "@" +  BINANCE_STREAM_TYPE;
            JsonObject subscribeRequest = new JsonObject()
                    .put ("method", "SUBSCRIBE")
                    .put ("params", new JsonArray().add(streamName))
                    .put ("id",  requestID );

            String messageToSend = subscribeRequest.encode();

            webSocket.writeTextMessage(messageToSend,  msg -> {
                if (msg.succeeded()) {
                    System.out.println("Subscribe message sent for: " + symbol);
                }else  {
                    System.err.println("Could not subscribe to: " + symbol +  " " + msg.cause().getMessage());
                }
            });
        });

        router.post("/unsubscribe/:symbol").handler(event -> {

            String symbol = event.pathParam("symbol");

            if (symbol == null || symbol.isEmpty()) {
                System.out.println("Invalid symbol received");
                return;
            }
            JsonObject delete = new JsonObject().put("symbol", symbol);
            if(!subscribedSymbols.contains(delete.getString("symbol"))) {
                event.response().setStatusCode(500).putHeader("content-type", "text-plain").end("Symbol is not subscribed");
                return;
            }

            mongoClient.removeDocument(SUBS_COLLECTION,delete,res ->{
                if (res.succeeded()) {
                    subscribedSymbols.remove(symbol);
                    event.response().setStatusCode(200).end("Unsubscribed to " + symbol);
                    System.out.println("Subscription removed from MongoDB for :" + symbol);
                }else {
                    event.response().setStatusCode(500).end("Could not unsubscribe to: " + symbol +  " " + res.cause().getMessage());
                    System.err.println("Could not unsubscribe to: " + symbol +  " " + res.cause().getMessage());
                }
            });
            int requestID = 1;
            String streamName = symbol.toLowerCase() + "@" +  BINANCE_STREAM_TYPE;
            JsonObject unsubscribeRequest = new JsonObject()
                    .put ("method", "UNSUBSCRIBE")
                    .put ("params", new JsonArray().add(streamName))
                    .put ("id",  requestID );
            String messageToSend = unsubscribeRequest.encode();
            webSocket.writeTextMessage(messageToSend,  msg -> {
                if (msg.succeeded()) {
                    System.out.println("Unsubscribe message sent for: " + symbol);
                }else {
                    System.err.println("Could not subscribe to: " + symbol + " " + msg.cause().getMessage());
                }
            });


        });


        server.requestHandler(router);
        server.listen(8888, res -> {
            if (res.succeeded()) {
                System.out.println("Server started on port 8888");

            }else {
                System.out.println("Failed to start server on port 8888" + res.cause().getMessage());

            }});


    }


    private void connectToBinance(Promise<Void> startPromise) {

        httpClient.webSocket("/ws", result -> {
            if (result.succeeded()) {
                this.webSocket = result.result();
                System.out.println("Connected to Binance");

                this.webSocket.handler(buffer -> {
                    String message = buffer.toString();
                    saveMessage(message);
                });

                if(!subscribedSymbols.isEmpty()){
                    JsonArray streams = new JsonArray();
                    for (String symbol : subscribedSymbols) {
                        streams.add(symbol.toLowerCase() + "@" +  BINANCE_STREAM_TYPE);

                    }
                    JsonObject subscribeRequest = new JsonObject()
                            .put ("method", "SUBSCRIBE").put ("params", streams).put("id",  1);

                    webSocket.writeTextMessage(subscribeRequest.encode(), res -> {
                        if (res.succeeded()) {
                            System.out.println("Subscribing to the list");
                        }else {
                            System.out.println("Failed to subscribe to the list");
                        }
                    });
                }
                startPromise.complete();
            } else {
                System.err.println("Failed to connect to Binance WebSocket: " + result.cause().getMessage());
                startPromise.fail(result.cause());
            }
        });
    }

    private void saveMessage(String message) {
        JsonObject json = new JsonObject(message);

        String symbol = json.getString("s");
        String price = json.getString("p");
        Long  timestamp = json.getLong("T");

        if(symbol != null && price != null) {
            JsonObject priceMessage = new JsonObject()
                    .put("symbol", symbol.toLowerCase())
                    .put("price", price)
                    .put("timestamp", timestamp);

            mongoClient.insert(COLLECTION_NAME, priceMessage, res -> {
                if (res.succeeded()) {
                    System.out.println("Saved price: " + price + " to " + symbol + " " + timestamp);
                }else {
                    System.err.println("Failed to save price: " + symbol + " " + timestamp);
                }
            });
        }else {
            System.err.println("Invalid symbol or price received");
        }


    }

}
