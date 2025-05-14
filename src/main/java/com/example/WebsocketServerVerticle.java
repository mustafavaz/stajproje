package com.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class WebsocketServerVerticle extends AbstractVerticle {
    private Set<ServerWebSocket>  clients = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    public void start(Promise<Void> startPromise)  {

        startServer(startPromise);
        eventBus();

    }

    private void startServer(Promise<Void> startPromise) {
        HttpServer server = vertx.createHttpServer();
        server.webSocketHandler(socket -> {
            clients.add(socket);
            socket.writeTextMessage("Client connected");
            System.out.println("Client connected");

            socket.closeHandler(closeHandler -> {
                clients.remove(socket);
                socket.writeTextMessage("Client disconnected ");
                System.out.println("Client disconnected" );
            });

        }).listen(8888, res -> {
            if (res.succeeded()) {
                System.out.println("Websocket server started on port 8888");
                startPromise.complete();
            }else  {
                startPromise.fail(res.cause());
            }
        });
    }
    public void eventBus(){
        vertx.eventBus().consumer("trades", msg -> {
            String data = msg.body().toString();

            clients.forEach(socket  -> socket.writeTextMessage(data));
        });
    }






}
