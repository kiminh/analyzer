package com.cpc.spark.novel;
import com.cpc.spark.novel.MapFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import java.util.Map;

public  class CryptBase64Utils {
    public static final Map<Byte, Byte> cryptMap = new MapFactory<Byte, Byte>()
            .append((byte) 'A', (byte) 'l').append((byte) 'B', (byte) 'T').append((byte) 'C', (byte) 'o').append((byte) 'D', (byte) 'W')
            .append((byte) 'E', (byte) 'f').append((byte) 'F', (byte) 'M').append((byte) 'G', (byte) 'p').append((byte) 'H', (byte) 'E')
            .append((byte) 'I', (byte) 'H').append((byte) 'J', (byte) 'A').append((byte) 'K', (byte) 'c').append((byte) 'L', (byte) 'i')
            .append((byte) 'M', (byte) 'r').append((byte) 'N', (byte) '_').append((byte) 'O', (byte) 'J').append((byte) 'P', (byte) 'L')
            .append((byte) 'Q', (byte) 'R').append((byte) 'R', (byte) 'd').append((byte) 'S', (byte) 'B').append((byte) 'T', (byte) 'D')
            .append((byte) 'U', (byte) 'S').append((byte) 'V', (byte) 'k').append((byte) 'W', (byte) 'a').append((byte) 'X', (byte) 'U')
            .append((byte) 'Y', (byte) '5').append((byte) 'Z', (byte) 'h').append((byte) 'a', (byte) 'P').append((byte) 'b', (byte) 'n')
            .append((byte) 'c', (byte) 'm').append((byte) 'd', (byte) 'O').append((byte) 'e', (byte) 'C').append((byte) 'f', (byte) 'q')
            .append((byte) 'g', (byte) 'V').append((byte) 'h', (byte) 'Z').append((byte) 'i', (byte) '2').append((byte) 'j', (byte) 'K')
            .append((byte) 'k', (byte) 'X').append((byte) 'l', (byte) 'N').append((byte) 'm', (byte) 'z').append((byte) 'n', (byte) 'I')
            .append((byte) 'o', (byte) '4').append((byte) 'p', (byte) 'u').append((byte) 'q', (byte) 'w').append((byte) 'r', (byte) 'G')
            .append((byte) 's', (byte) 'x').append((byte) 't', (byte) '0').append((byte) 'u', (byte) '1').append((byte) 'v', (byte) 'Y')
            .append((byte) 'w', (byte) 's').append((byte) 'x', (byte) '3').append((byte) 'y', (byte) '7').append((byte) 'z', (byte) '9')
            .append((byte) '0', (byte) 'e').append((byte) '1', (byte) 'g').append((byte) '2', (byte) 't').append((byte) '3', (byte) 'Q')
            .append((byte) '4', (byte) '8').append((byte) '5', (byte) 'y').append((byte) '6', (byte) 'F').append((byte) '7', (byte) '-')
            .append((byte) '8', (byte) 'v').append((byte) '9', (byte) 'b').append((byte) '-', (byte) 'j').append((byte) '_', (byte) '6')
            .append((byte) '=', (byte) '=').getMap();

    public static final Map<Byte, Byte> reverseCryptMap = new MapFactory<Byte, Byte>().putReverseMap(cryptMap).getMap();

    public static String cryptBase64ToString(String cryptBase64) {
        if (! cryptBase64.isEmpty()) {
            return cryptBase64ToString(cryptBase64.getBytes());
        }
        return null;
    }

    public static String cryptBase64ToString(byte[] cryptBase64) {
        return new String(cryptBase64ToByteArray(cryptBase64));
    }

    public static byte[] cryptBase64ToByteArray(byte[] cryptBase64) {

        if (cryptBase64 == null || cryptBase64.length <= 0) {
            return null;
        }

        byte newByteArray[] = new byte[cryptBase64.length];

        for (int counter = 0; counter < cryptBase64.length; counter++) {
            byte pos = cryptBase64[counter];
            Byte targetPos = reverseCryptMap.get(pos);
            if (targetPos != null) {
                newByteArray[counter] = targetPos;
            } else {
                newByteArray[counter] = pos;
            }
        }

        return Base64.decodeBase64(newByteArray);
    }

    public static byte[] byteArrayToCryptBase64(byte[] byteArray) {
        if (byteArray == null || byteArray.length <= 0)
            return null;

        byte[] encode = Base64.encodeBase64(byteArray);

        for (int counter = 0; counter < encode.length; counter++) {
            byte pos = encode[counter];
            Byte targetPos = cryptMap.get(pos);
            if (targetPos != null)
                encode[counter] = targetPos;
            else
                encode[counter] = pos;
        }

        return encode;
    }

    public static String byteArrayToCryptBase64String(byte[] byteArray) {
        return new String(byteArrayToCryptBase64(byteArray));
    }

    public static String StringToCryptBase64String(String str) {
        return byteArrayToCryptBase64String(str.getBytes());
    }

}

