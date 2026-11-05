#!/usr/bin/env bash
# Checks the Linux kernel that Docker/kind run on: eBPF CO-RE needs BTF, the agent needs
# kernel >= 5.15 and cgroup v2 (requirements.md, section 6.2).
set -euo pipefail

readonly MIN_MAJOR=5
readonly MIN_MINOR=15

report=$(docker run --rm alpine:3 sh -c '
  echo "kernel=$(uname -r)"
  echo "arch=$(uname -m)"
  if [ -r /sys/kernel/btf/vmlinux ]; then echo "btf=yes"; else echo "btf=no"; fi
  echo "cgroupfs=$(awk '\''$2 == "/sys/fs/cgroup" {print $3}'\'' /proc/mounts)"
')

echo "${report}"

kernel=$(grep '^kernel=' <<<"${report}" | cut -d= -f2)
major=$(cut -d. -f1 <<<"${kernel}")
minor=$(cut -d. -f2 <<<"${kernel}")

status=0
if (( major < MIN_MAJOR || (major == MIN_MAJOR && minor < MIN_MINOR) )); then
  echo "FAIL: kernel ${kernel} < ${MIN_MAJOR}.${MIN_MINOR}"
  status=1
fi
if ! grep -q '^btf=yes' <<<"${report}"; then
  echo "FAIL: /sys/kernel/btf/vmlinux missing — use a Lima/Colima VM with Ubuntu 24.04"
  status=1
fi
if ! grep -q '^cgroupfs=cgroup2$' <<<"${report}"; then
  echo "FAIL: cgroup v2 not mounted"
  status=1
fi

if (( status == 0 )); then
  echo "OK: kernel supports eBPF CO-RE for faultline-agent"
fi
exit "${status}"
