package com.example;

import io.vertx.core.Vertx;

public class Launcher {
    public static void main(String[] args) {

        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new BinanceWebsocketVerticle(), deploymentResult -> {
            if (deploymentResult.succeeded()) {
                System.out.println("Websocket verticle deployed");
            } else {
                System.err.println("Websocket verticle failed: " + deploymentResult.cause().getMessage());
            }
        });
        vertx.deployVerticle(new DataAccessVerticle(), deploymentResult -> {
            if (deploymentResult.succeeded()) {
                System.out.println("DataAccess verticle deployed");
            }else  {
                System.err.println("DataAccess verticle failed: " + deploymentResult.cause().getMessage());
            }
        });
    }
}