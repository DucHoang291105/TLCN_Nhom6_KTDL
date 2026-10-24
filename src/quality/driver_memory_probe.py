"""Read cgroup evidence around the A-owned Spark driver resource change."""
import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from src.common.atomic_io import write_json_atomic

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("label", choices=("before", "after"))
    args = parser.parse_args()
    def read(name):
        return subprocess.check_output(["docker", "exec", "tlcn_spark_master", "cat", "/sys/fs/cgroup/"+name], text=True).strip()
    events = {line.split()[0]: int(line.split()[1]) for line in read("memory.events").splitlines()}
    measured = {"generated_at": datetime.now(timezone.utc).isoformat(), "events": events,
                "memory_limit_bytes": int(read("memory.max")), "peak_bytes": int(read("memory.peak")),
                "swap_bytes": int(read("memory.swap.current"))}
    mounts = json.loads(subprocess.check_output(["docker", "inspect", "--format", "{{json .Mounts}}", "tlcn_spark_master"], text=True))
    measured["ivy_named_volume"] = any(m["Type"] == "volume" and m["Destination"] == "/home/spark/.ivy2" for m in mounts)
    measured["ivy_cache_writable"] = subprocess.run(["docker", "exec", "--user", "185:185", "tlcn_spark_master",
        "sh", "-c", "test -w /home/spark/.ivy2 && test -d /home/spark/.ivy2/cache && test -d /home/spark/.ivy2/jars"], check=False).returncode == 0
    path = ROOT / "docs/validation/person_a_driver_memory.json"
    payload = json.loads(path.read_text()) if path.exists() else {}
    payload[args.label] = measured
    write_json_atomic(path, payload)
    print(json.dumps(measured, indent=2))


if __name__ == "__main__":
    main()
