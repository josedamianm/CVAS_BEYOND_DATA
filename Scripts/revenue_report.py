#!/usr/bin/env python3
import csv
import argparse
import os
from collections import defaultdict
from datetime import datetime

DEFAULT_CSV = os.path.join(os.path.dirname(__file__), '..', 'Counters', 'Counters_Service.csv')

def load_data(csv_path):
    with open(csv_path) as f:
        return list(csv.DictReader(f))

def generate_report(rows, services, months):
    data = defaultdict(lambda: defaultdict(float))

    for r in rows:
        svc = r['service_name'].strip()
        if svc not in services:
            continue
        try:
            d = datetime.strptime(r['date'].strip(), '%Y-%m-%d')
        except ValueError:
            continue
        month = d.strftime('%Y-%m')
        if month not in months:
            continue
        data[svc][month] += float(r['rev'])

    col_w = 12
    svc_w = max(len(s) for s in services) + 2

    header = f"{'Service':<{svc_w}}"
    for m in months:
        header += f"{m:>{col_w}}"
    header += f"{'TOTAL':>{col_w}}"
    print(header)
    print("-" * (svc_w + col_w * (len(months) + 1)))

    for svc in services:
        line = f"{svc:<{svc_w}}"
        total = 0.0
        for m in months:
            rev = data[svc][m]
            total += rev
            line += f"{rev:>{col_w},.0f}"
        line += f"{total:>{col_w},.0f}"
        print(line)

def main():
    parser = argparse.ArgumentParser(description='Generate monthly revenue report by service.')
    parser.add_argument(
        '-s', '--services',
        nargs='+',
        required=True,
        help='One or more service names (e.g. "IntimaX" "Slow Life")'
    )
    parser.add_argument(
        '-m', '--months',
        nargs='+',
        required=True,
        help='One or more months in YYYY-MM format (e.g. 2025-09 2025-10)'
    )
    parser.add_argument(
        '-f', '--file',
        default=DEFAULT_CSV,
        help=f'Path to Counters_Service.csv (default: {DEFAULT_CSV})'
    )
    args = parser.parse_args()

    for m in args.months:
        try:
            datetime.strptime(m, '%Y-%m')
        except ValueError:
            parser.error(f"Invalid month format '{m}', expected YYYY-MM")

    rows = load_data(args.file)
    months = sorted(args.months)
    generate_report(rows, args.services, months)

if __name__ == '__main__':
    main()
