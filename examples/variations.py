"""Build the table-variations lab in the demo warehouse, for trying iceops by hand.

Creates the same tables the compatibility matrix (tests/test_variations.py) validates —
partition transforms, spec/schema evolution, copy-on-write overwrites — under the 'lab'
namespace of the demo catalog.

Run:  uv run python examples/demo.py        (first, if you haven't)
      uv run python examples/variations.py
"""

from __future__ import annotations

from pathlib import Path

from pyiceberg.catalog import load_catalog
from table_factory import VARIATIONS, build_all

ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE = ROOT / "demo_warehouse"


def main() -> None:
    if not WAREHOUSE.exists():
        raise SystemExit("demo warehouse missing — run: uv run python examples/demo.py")

    catalog = load_catalog(
        "demo",
        type="sql",
        uri=f"sqlite:///{WAREHOUSE}/catalog.db",
        warehouse=f"file://{WAREHOUSE}",
    )
    print("building the variations lab ...")
    results = build_all(catalog, "lab")

    described = {f"lab.{v.name}": v.describe for v in VARIATIONS}
    for identifier, status in sorted(results.items()):
        marker = "ok " if status == "ok" else "!! "
        print(f"  {marker}{identifier:<22} {described[identifier]}")
        if status != "ok":
            print(f"      {status}")

    print("\ntry:")
    print("  uv run iceops scan --catalog demo --pattern 'lab.*'")
    print("  uv run iceops doctor lab.part_day --catalog demo")
    print("  uv run iceops cost lab.overwritten --catalog demo   # stale bytes are real here")
    print("  uv run iceops rewrite-manifests lab.evolved_spec --catalog demo")
    print("  uv run iceops expire lab.overwritten --catalog demo --retain-last 1 --older-than 0s")


if __name__ == "__main__":
    main()
