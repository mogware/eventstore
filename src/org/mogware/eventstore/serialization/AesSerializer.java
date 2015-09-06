package org.mogware.eventstore.serialization;

import java.io.InputStream;
import java.io.OutputStream;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class AesSerializer implements Serialize {
    private final byte[] encryptionKey;
    private final Serialize inner;

    public AesSerializer(Serialize inner, byte[] encryptionKey) {
        this.encryptionKey = encryptionKey;
        this.inner = inner;
    }

    @Override
    public <T> void serialize(OutputStream output, T graph) {
        try {
            Cipher cipher = getCipherEncrypt(this.encryptionKey);
            this.inner.serialize(new CipherOutputStream(output, cipher), graph);
        } catch (Exception ex) {
        }
    }

    @Override
    public <T> T deserialize(InputStream input) {
        try {
            Cipher cipher = getCipherDecrypt(this.encryptionKey);
            return inner.deserialize(new CipherInputStream(input, cipher));
        } catch (Exception ex) {
            return null;
        }
    }

    private byte[] getKeyBytes(final byte[] key) throws Exception {
        byte[] keyBytes = new byte[16];
        System.arraycopy(key, 0, keyBytes, 0,
                Math.min(key.length, keyBytes.length));
        return keyBytes;
    }

    private Cipher getCipherEncrypt(final byte[] key) throws Exception {
        byte[] keyBytes = getKeyBytes(key);
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        SecretKeySpec secretKeySpec = new SecretKeySpec(keyBytes, "AES");
        IvParameterSpec ivParameterSpec = new IvParameterSpec(keyBytes);
        cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivParameterSpec);
        return cipher;
    }

    private Cipher getCipherDecrypt(byte[] key) throws Exception {
        byte[] keyBytes = getKeyBytes(key);
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        SecretKeySpec secretKeySpec = new SecretKeySpec(keyBytes, "AES");
        IvParameterSpec ivParameterSpec = new IvParameterSpec(keyBytes);
        cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, ivParameterSpec);
        return cipher;
    }
}
