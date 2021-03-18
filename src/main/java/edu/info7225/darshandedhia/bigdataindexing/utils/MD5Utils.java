package edu.info7225.darshandedhia.bigdataindexing.utils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Utils {
    private static final Charset UTF_8 = StandardCharsets.UTF_8;
    private static final String OUTPUT_FORMAT = "%-20s:%s";

    private static byte[] digest(byte[] input) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] result = md.digest(input);
        return result;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    public static String hashString(String data) throws NoSuchAlgorithmException{
        byte[] md5InBytes = MD5Utils.digest(data.getBytes(UTF_8));
        String bytesToHex = MD5Utils.bytesToHex(md5InBytes);

        return bytesToHex;
    }
}
