package dk.nsi.sdm4.core.util;

import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Generator {

    public static String makeMd5Identifier(String identifier) {
        String result;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.reset();
            byte[] digest = md.digest(identifier.getBytes());
            result = String.valueOf(Hex.encodeHex(digest));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

}
