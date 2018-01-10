package com.example;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IPMatcher extends UDF {
    private final BooleanWritable result = new BooleanWritable();

    public BooleanWritable evaluate(final Text ip, final Text network) {
        return matches(ip.toString(), network.toString());
    }

    public BooleanWritable matches(final String ip, final String network) {
        final long maxAddr = getMaxAddr(network);
        final long minAddr = getMinAddr(network);
        final long numericIp = ipToLong(ip);
        result.set(numericIp <= maxAddr && numericIp >= minAddr);
        return result;
    }

    private long getMaxAddr(final String network) {
        final String ip = numericIpToBinary(network.split("/")[0]);
        final Integer mask = Integer.parseInt(network.split("/")[1]);
        final String maxAddrBinary = ip.substring(0, mask) + IntStream.range(mask, 32).mapToObj(i -> "1").collect(Collectors.joining());
        return Long.parseUnsignedLong(maxAddrBinary, 2);
    }

    private long getMinAddr(final String network) {
        final String ip = numericIpToBinary(network.split("/")[0]);
        final Integer mask = Integer.parseInt(network.split("/")[1]);
        final String minAddrBinary = ip.substring(0, mask) + IntStream.range(mask, 32).mapToObj(i -> "0").collect(Collectors.joining());
        return Long.parseUnsignedLong(minAddrBinary, 2);
    }

    private long ipToLong(final String ip) {
        return Long.parseUnsignedLong(numericIpToBinary(ip), 2);
    }

    private String numericIpToBinary(final String ip) {
        return Arrays.stream(ip.split("\\."))
                .map(Integer::parseInt)
                .map(this::longToBinWithLength)
                .collect(Collectors.joining());

    }

    private String longToBinWithLength(final int segment) {
        final String binary = Integer.toBinaryString(segment);
        if (binary.length() < 8) {
            return IntStream.range(0, 8 - binary.length())
                    .mapToObj(i -> "0")
                    .collect(Collectors.joining()) + binary;
        }
        return binary;
    }

}
