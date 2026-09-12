"""
Quick sanity check: confirms the producer's --rate setting hits the
1M+ events/day target, and reports what daily volume a given rate produces.

Usage:
    python tests/throughput_check.py --rate 12
"""

import argparse


def main(args):
    events_per_day = args.rate * 60 * 60 * 24
    print(f"Rate: {args.rate} events/sec")
    print(f"  -> {args.rate * 60:.0f} events/minute")
    print(f"  -> {args.rate * 3600:,.0f} events/hour")
    print(f"  -> {events_per_day:,.0f} events/day")

    target = 1_000_000
    if events_per_day >= target:
        print(f"\nMeets the {target:,} events/day target ✔")
    else:
        needed_rate = target / (60 * 60 * 24)
        print(f"\nBelow the {target:,} events/day target. Use --rate {needed_rate:.2f} or higher.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=12.0, help="Events per second from the producer")
    args = parser.parse_args()
    main(args)
