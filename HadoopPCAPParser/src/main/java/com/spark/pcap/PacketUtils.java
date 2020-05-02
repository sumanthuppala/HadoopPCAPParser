package com.spark.pcap;

import net.ripe.hadoop.pcap.packet.Packet;

import java.util.ArrayList;
import java.util.List;

public class PacketUtils {
    public static String packetToString(Packet packet) {
        String[] fields = {Packet.SRC, Packet.SRC_PORT, Packet.DST, Packet.DST_PORT, Packet.PROTOCOL};
        List<String> output = new ArrayList<>();
        for(int i=0; i<fields.length; i++) {
            output.add(packet.get(fields[i]).toString());
        }

        return String.join(",", output);
    }
}
