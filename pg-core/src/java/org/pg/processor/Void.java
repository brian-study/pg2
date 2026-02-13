package org.pg.processor;

import org.pg.codec.CodecParams;
import java.nio.ByteBuffer;

/**
 * Processor for the VOID type (OID 2278).
 * PostgreSQL returns VOID for functions with no meaningful return value
 * (e.g., pg_notify). The value is always empty/null.
 */
public class Void extends AProcessor {

    @Override
    public Object decodeBin(final ByteBuffer bb, final CodecParams codecParams) {
        return null;
    }

    @Override
    public Object decodeTxt(final String text, final CodecParams codecParams) {
        return null;
    }

    @Override
    public ByteBuffer encodeBin(final Object x, final CodecParams codecParams) {
        return ByteBuffer.allocate(0);
    }

    @Override
    public String encodeTxt(final Object x, final CodecParams codecParams) {
        return "";
    }
}
