"""`amel-train train | promote | show` — the production entry points.
No notebook is involved anywhere in this path."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import asdict

from amel_training.config import TrainingConfig
from amel_training.promote import describe, promote, to_json
from amel_training.train import run_training


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="amel-train", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser(
        "train", help="run one training run; registers the model and aliases it candidate"
    )

    p_promote = sub.add_parser(
        "promote", help="promote candidate (or --version N) to champion if criteria pass"
    )
    p_promote.add_argument(
        "--version", default=None, help="explicit version instead of the candidate alias"
    )
    p_promote.add_argument("--decided-by", required=True, help="who is making this call (audit)")
    p_promote.add_argument(
        "--force", action="store_true", help="promote even if criteria fail (recorded as forced)"
    )

    sub.add_parser("show", help="print current candidate/champion state from the registry")

    args = parser.parse_args(argv)
    cfg = TrainingConfig()

    if args.command == "train":
        result = run_training(cfg)
        print(json.dumps(asdict(result), indent=2))
        return 0
    if args.command == "promote":
        decision = promote(cfg, version=args.version, decided_by=args.decided_by, force=args.force)
        print(to_json(asdict(decision)))
        print("PROMOTED" if (decision.approved or args.force) else "REJECTED", file=sys.stderr)
        return 0 if (decision.approved or args.force) else 2
    if args.command == "show":
        print(to_json(describe(cfg)))
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
