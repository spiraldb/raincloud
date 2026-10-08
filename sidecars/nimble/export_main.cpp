// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
// raincloud-export-nimble-cpp: raincloud's WRITE sidecar contract, over upstream Nimble.
#include "nimble_lane.h"

int main(int argc, char** argv) {
  return raincloud_nimble_write_main(argc, argv, nimble_lane_write, nimble_lane_read);
}
