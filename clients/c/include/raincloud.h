/* SPDX-License-Identifier: Apache-2.0 */
#ifndef RAINCLOUD_H
#define RAINCLOUD_H
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
struct ArrowArrayStream;
typedef struct raincloud_dataset raincloud_dataset;
typedef struct { int32_t code; char *message; } raincloud_error;
/* Error codes. The numbers are ABI: codes are only ever appended, never
 * renumbered or reused, and a caller must treat a code it does not know as
 * RAINCLOUD_INTERNAL. */
enum raincloud_error_code {
  RAINCLOUD_OK=0, RAINCLOUD_INVALID_ARGUMENT=1, RAINCLOUD_CATALOG=2,
  RAINCLOUD_MISSING_REVISION=3, RAINCLOUD_UNKNOWN_SLUG=4,
  RAINCLOUD_FORMAT_UNAVAILABLE=5, RAINCLOUD_OFFLINE_MISS=6,
  RAINCLOUD_ARTIFACT_NOT_FOUND=7, RAINCLOUD_CHECKSUM_MISMATCH=8,
  /* Producer-side (build/publish); a read never returns it. */
  RAINCLOUD_CATALOG_CONFLICT=9,
  /* The mirror could not be read: unreachable, refused, or not a store. A
   * mirror that answers "no such file" is ARTIFACT_NOT_FOUND. */
  RAINCLOUD_TRANSPORT=10,
  /* The bytes are fine and this build cannot represent their type: ask for
   * another format. */
  RAINCLOUD_UNSUPPORTED_TYPE=11, RAINCLOUD_IO=12, RAINCLOUD_INTERNAL=13,
  /* Found but undecodable: truncated, damaged, or not the claimed format.
   * Re-fetch or rebuild it; another format will not help. */
  RAINCLOUD_CORRUPT_ARTIFACT=14
};
/* ABI changes are additive. The version rises when an entry point is added,
 * so a client that calls an entry point introduced at version N checks
 * raincloud_abi_version() >= N. Appending an error code does not raise it
 * (see above). An existing entry point, struct or code never changes meaning
 * under this library name: an incompatible change would ship as a library
 * with a new name, never as a higher version of this one. */
uint32_t raincloud_abi_version(void);
/* Every open/path/batches call runs one `raincloud` process and blocks until
 * it exits. The process inherits this one's environment, working directory
 * (relative settings resolve against it) and stderr, and may wait on the
 * store's download lock with no timeout.
 *
 * UTF-8 options JSON: Raincloud settings (the TOML keys, plus config and
 * no_config), which override environment/TOML. NULL = {}. Catalog selection,
 * resolution, downloads and verification run in the `raincloud` CLI: the
 * "cli" option names it, else RAINCLOUD_CLI, else PATH. Settings reach it
 * through its environment, never its command line.
 * format: auto, arrow, parquet, or vortex; auto chooses among the formats
 * this library was built to decode.
 * open loads catalog metadata only. No artifact I/O or implicit builds.
 * Outputs must be writable; errors must be zero-initialized and freed before
 * reuse. A NULL error pointer discards details; return code always reports failure.
 * Handles are immutable; do not close one while another thread is using it.
 * A handle stays on the catalog generation it opened with: path and batches
 * return RAINCLOUD_CATALOG if the selected catalog has changed since. */
int32_t raincloud_open(const char *options, const char *slug, const char *format,
                      raincloud_dataset **out, raincloud_error *error);
/* Strings returned on success belong to caller; release with string_free. */
int32_t raincloud_metadata(const raincloud_dataset *, char **out, raincloud_error *);
int32_t raincloud_path(const raincloud_dataset *, char **out, raincloud_error *);
/* out must be uninitialized/released ArrowArrayStream storage. Stream owns
 * reader independently of dataset handle. Use standard Arrow stream callbacks;
 * release every schema, array and stream exactly once. Consuming concurrently
 * from the same stream is unsupported. batch_size > 0; encoded chunk sizes may
 * exceed this output row limit. Stream callback errors use get_last_error().
 * Stream callbacks never unwind: a decoder panic (damaged bytes or a decoder
 * bug; the two cannot be told apart) is an error from get_next, after which
 * the stream is at its end. */
int32_t raincloud_batches(const raincloud_dataset *, size_t batch_size,
                         struct ArrowArrayStream *out, raincloud_error *);
void raincloud_close(raincloud_dataset *);
void raincloud_string_free(char *);
void raincloud_error_free(raincloud_error *);
#ifdef __cplusplus
}
#endif
#endif
