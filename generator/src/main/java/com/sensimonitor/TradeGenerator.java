package com.sensimonitor;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TradeGenerator {

    private static final List<String> DESKS = List.of("RATES", "FX", "CREDIT", "EQUITY");
    private static final List<String> CURRENCIES = List.of("EUR", "USD", "GBP", "JPY");

    private String messySource(String source) {
        List<String> messySources = List.of(source.toUpperCase(), source.toLowerCase(), Character.toUpperCase(source.charAt(0))+source.substring(1).toLowerCase());
        int random =  ThreadLocalRandom.current().nextInt(messySources.size());
        return messySources.get(random);
    }
    public Trade randomTrade(String source, String prefix, int index) {
        String tradeId = String.format("%s-%06d",prefix,index);
        source = messySource(source);
        String desk = DESKS.get(ThreadLocalRandom.current().nextInt(DESKS.size()));
        String currency = CURRENCIES.get(ThreadLocalRandom.current().nextInt(CURRENCIES.size()));
        double delta = Math.round(ThreadLocalRandom.current().nextDouble(-10000,10000) * 100) / 100.0;
        double vega = Math.round(ThreadLocalRandom.current().nextDouble(-2000,2000) * 100) / 100.0;
        String date = LocalDate.now().toString();


        return new Trade(
                tradeId,
                source,
                desk,
                currency,
                delta,
                vega,
                date
        );
    }
}
