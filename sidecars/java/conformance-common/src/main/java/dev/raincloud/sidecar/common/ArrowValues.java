// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** Helpers for reading arrow-java {@code getObject} logical values robustly. */
public final class ArrowValues {
    private ArrowValues() {}

    /**
     * An Int-family {@code getObject} value as an EXACT integer, unsigned-aware at
     * every width including 64.
     *
     * <p>arrow-java boxes {@code UInt2Vector} (uint16) as {@link Character} and the
     * other widths as {@code Byte/Short/Integer/Long} holding the RAW bits, so a
     * uint8 of 162 arrives as {@code (byte) -94} and a uint64 of
     * {@code 18446744073709551615} as {@code -1L}. No {@code long} can carry every
     * uint64, and a raw-bits carrier would compare that uint64 EQUAL to a signed
     * int64 of {@code -1}; the comparison carrier is therefore exact.
     */
    public static java.math.BigInteger intAsExact(ArrowType.Int type, Object v) {
        long raw = (v instanceof Character) ? (long) (char) (Character) v : ((Number) v).longValue();
        int bits = type.getBitWidth();
        if (type.getIsSigned()) {
            return java.math.BigInteger.valueOf(raw);
        }
        if (bits < 64) {
            return java.math.BigInteger.valueOf(raw & ((1L << bits) - 1));
        }
        // uint64: reinterpret the 64 raw bits as unsigned (no long can hold it).
        return new java.math.BigInteger(Long.toUnsignedString(raw));
    }

    /**
     * A FloatingPoint-family {@code getObject} value as the double it denotes.
     *
     * <p>{@code Float2Vector.getObject} returns the RAW IEEE-754 half bits as a
     * {@link Short} (arrow-vector 19), so {@code Number.doubleValue()} would read a
     * half 1.0 ({@code 0x3C00}) as 15360.0. Half values are decoded; float and double
     * widen exactly.
     */
    public static double floatAsDouble(ArrowType.FloatingPoint type, Object v) {
        if (type.getPrecision() == FloatingPointPrecision.HALF) {
            return halfToFloat((Short) v);
        }
        return ((Number) v).doubleValue();
    }

    /**
     * IEEE-754 binary16 bits to the float with the same value: signed zeros,
     * subnormals, infinities and NaN payloads included. (JDK 20 has
     * {@code Float.float16ToFloat}; the sidecars target JDK 17.)
     */
    static float halfToFloat(short bits) {
        int h = bits & 0xFFFF;
        int sign = (h & 0x8000) << 16;
        int exponent = (h >>> 10) & 0x1F;
        int mantissa = h & 0x3FF;
        if (exponent == 0x1F) {
            return Float.intBitsToFloat(sign | 0x7F80_0000 | (mantissa << 13));
        }
        if (exponent == 0) {
            // Zero or subnormal: mantissa * 2^-24, exact in float.
            float magnitude = mantissa * 0x1p-24f;
            return sign == 0 ? magnitude : -magnitude;
        }
        return Float.intBitsToFloat(sign | ((exponent + 112) << 23) | (mantissa << 13));
    }
}
