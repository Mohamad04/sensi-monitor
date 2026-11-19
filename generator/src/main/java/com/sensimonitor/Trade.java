package com.sensimonitor;

public record Trade (String tradeId,
        String source,
        String desk,
        String currency,
        double delta,
        double vega,
        String asOfDate)
{


}
