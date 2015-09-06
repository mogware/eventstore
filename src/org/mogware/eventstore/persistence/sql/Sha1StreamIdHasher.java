package org.mogware.eventstore.persistence.sql;

import java.security.MessageDigest;

public class Sha1StreamIdHasher implements StreamIdHasher {
    @Override
    public String GetHash(String streamId) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            md.update(streamId.getBytes());
            byte[] hashBytes = md.digest();
            return bytesToHex(hashBytes);
        } catch (Exception e) {
            return streamId;
        }         
    }
    
    private static String bytesToHex(byte[] b) {
        char hexDigit[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
        StringBuilder buf = new StringBuilder();
        for (int j=0; j<b.length; j++) {
            buf.append(hexDigit[(b[j] >> 4) & 0x0f]);
            buf.append(hexDigit[b[j] & 0x0f]);
        }
        return buf.toString();
   }    
}
