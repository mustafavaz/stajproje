package com.example;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SubscribedSymbols {
    private static final SubscribedSymbols instance = new  SubscribedSymbols();


    private final Set<String> subscribedSymbols = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private SubscribedSymbols() {

    }

    public static SubscribedSymbols getInstance() {
        return instance;
    }
    public static Set<String> getSubscribedSymbols() {
        return instance.subscribedSymbols;
    }
    public void addSymbol(String symbol) {
        subscribedSymbols.add(symbol);
    }
    public void removeSymbol(String symbol) {
        subscribedSymbols.remove(symbol);
    }
    public boolean containsSymbol(String symbol) {
        return subscribedSymbols.contains(symbol.toLowerCase());
    }




}
