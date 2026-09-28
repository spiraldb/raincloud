// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud;

import java.util.Map;
import com.sun.jna.*;
import com.sun.jna.ptr.PointerByReference;

interface NativeApi extends Library {
    NativeApi INSTANCE = Native.load("raincloud_reader", NativeApi.class, Map.of(Library.OPTION_STRING_ENCODING, "UTF-8"));
    @Structure.FieldOrder({"code", "message"})
    class Failure extends Structure {
        public int code;
        public Pointer message;
    }
    // size_t follows pointer width, including Windows LLP64.
    class Size extends IntegerType { public Size() { this(0); } public Size(long value) { super(Native.SIZE_T_SIZE, value, true); } }
    int raincloud_abi_version();
    int raincloud_open(String options, String slug, String format, PointerByReference out, Failure error);
    int raincloud_metadata(Pointer handle, PointerByReference out, Failure error);
    int raincloud_path(Pointer handle, PointerByReference out, Failure error);
    int raincloud_batches(Pointer handle, Size batchSize, Pointer out, Failure error);
    void raincloud_close(Pointer handle);
    void raincloud_string_free(Pointer value);
    void raincloud_error_free(Failure error);

    static void check(int code, Failure error) {
        try {
            if (code != 0) throw new RaincloudException(code,
                error.message == null ? "Raincloud error" : error.message.getString(0, "UTF-8"));
        } finally { INSTANCE.raincloud_error_free(error); }
    }
}
