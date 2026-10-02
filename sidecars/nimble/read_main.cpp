// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
// raincloud-read-nimble-cpp: raincloud's READ sidecar contract, over upstream Nimble.
#include "nimble_lane.h"

int main(int argc, char** argv) { return raincloud_nimble_read_main(argc, argv, nimble_lane_read); }
