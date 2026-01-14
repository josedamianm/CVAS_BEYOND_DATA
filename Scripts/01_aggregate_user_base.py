#!/usr/bin/env python3
"""
Aggregate NBS Base data to calculate daily user base metrics.
Generates two output files:
1. user_base_by_service.csv - Daily user base by service_name and tme_category
2. user_base_by_category.csv - Daily user base by tme_category only
"""

import csv
import os
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# Configuration
PROJECT_ROOT = Path(__file__).parent.parent
NBS_BASE_DIR = PROJECT_ROOT / "User_Base" / "NBS_BASE"
SERVICE_OUTPUT = PROJECT_ROOT / "User_Base" / "user_base_by_service.csv"
CATEGORY_OUTPUT = PROJECT_ROOT / "User_Base" / "user_base_by_category.csv"

def extract_date_from_filename(filename):
    """Extract date from filename format: YYYYMMDD_NBS_Base.csv and convert to YYYY-MM-DD"""
    date_str = filename[:8]
    # Convert YYYYMMDD to YYYY-MM-DD
    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

def should_exclude_service(service_name):
    """Check if service should be excluded based on service name."""
    service_lower = service_name.lower()
    excluded_keywords = ['nubico', 'challenge arena', 'movistar apple music']
    return any(keyword in service_lower for keyword in excluded_keywords)

def map_category(category):
    """Map categories according to grouping rules (case-insensitive)."""
    # Normalize to title case for consistent mapping
    category_normalized = category.strip()

    category_mapping = {
        'education': 'Edu_Ima',
        'images': 'Edu_Ima',
        'news': 'News_Sport',
        'sports': 'News_Sport'
    }

    # Check lowercase version for mapping
    mapped = category_mapping.get(category_normalized.lower())

    # If mapped, return the mapped value; otherwise return original with proper title case
    if mapped:
        return mapped

    # Return original category (preserve original casing for unmapped categories)
    return category_normalized

def process_files():
    """Process all CSV files and aggregate data."""

    # Data structures to hold all aggregations
    # service_data: {(date, service_name, tme_category): user_base_count}
    # category_data: {(date, tme_category): user_base_count}
    service_data = defaultdict(int)
    category_data = defaultdict(int)

    # Get all CSV files sorted by date
    nbs_path = Path(NBS_BASE_DIR)
    csv_files = sorted(nbs_path.glob("*.csv"))

    total_files = len(csv_files)
    print(f"Found {total_files} CSV files to process\n")

    # Process each file
    for idx, csv_file in enumerate(csv_files, 1):
        filename = csv_file.name
        date = extract_date_from_filename(filename)

        # Progress indicator
        if idx % 100 == 0 or idx == total_files:
            print(f"Processing {idx}/{total_files}: {filename}")

        try:
            # Try UTF-8 first, fall back to Latin-1 if it fails
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            file_opened = False

            for encoding in encodings:
                try:
                    with open(csv_file, 'r', encoding=encoding) as f:
                        reader = csv.DictReader(f)

                        for row in reader:
                            service_name = row['service_name'].strip()

                            # Skip excluded services
                            if should_exclude_service(service_name):
                                continue

                            tme_category = row['tme_category'].strip()
                            count_val = int(float(row['count']))

                            # Map category to grouped category
                            mapped_category = map_category(tme_category)

                            # Aggregate by service and category
                            service_key = (date, service_name, mapped_category)
                            service_data[service_key] += count_val

                            # Aggregate by category only
                            category_key = (date, mapped_category)
                            category_data[category_key] += count_val

                    file_opened = True
                    break  # Successfully processed, exit encoding loop

                except UnicodeDecodeError:
                    if encoding == encodings[-1]:
                        raise  # If last encoding fails, raise the error
                    continue  # Try next encoding

            if not file_opened:
                print(f"WARNING: Could not open {filename} with any encoding")

        except Exception as e:
            print(f"ERROR processing {filename}: {e}")
            continue

    return service_data, category_data

def write_service_output(service_data, output_file):
    """Write service aggregation to CSV file."""
    print(f"\nWriting service aggregation to {output_file}...")

    with open(output_file, 'w', encoding='utf-8') as f:
        # Write header
        f.write("date|service_name|tme_category|User_Base\n")

        # Sort by date, service_name, tme_category
        sorted_data = sorted(service_data.items())

        for (date, service_name, tme_category), user_base in sorted_data:
            f.write(f"{date}|{service_name}|{tme_category}|{user_base}\n")

    print(f"✓ Written {len(service_data)} records")

def write_category_output(category_data, output_file):
    """Write category aggregation to CSV file."""
    print(f"\nWriting category aggregation to {output_file}...")

    with open(output_file, 'w', encoding='utf-8') as f:
        # Write header
        f.write("date|tme_category|User_Base\n")

        # Sort by date, tme_category
        sorted_data = sorted(category_data.items())

        for (date, tme_category), user_base in sorted_data:
            f.write(f"{date}|{tme_category}|{user_base}\n")

    print(f"✓ Written {len(category_data)} records")

def show_summary(service_output, category_output):
    """Display summary statistics and samples."""
    print("\n" + "="*60)
    print("PROCESSING COMPLETE")
    print("="*60)

    # File sizes
    service_size = os.path.getsize(service_output) / (1024 * 1024)
    category_size = os.path.getsize(category_output) / (1024 * 1024)

    print(f"\nOutput Files:")
    print(f"  {service_output}: {service_size:.2f} MB")
    print(f"  {category_output}: {category_size:.2f} MB")

    # Sample data from service file
    print(f"\n--- Sample from {service_output} (first 10 rows) ---")
    with open(service_output, 'r') as f:
        for i, line in enumerate(f):
            if i < 11:
                print(line.rstrip())

    # Sample data from category file
    print(f"\n--- Sample from {category_output} (first 10 rows) ---")
    with open(category_output, 'r') as f:
        for i, line in enumerate(f):
            if i < 11:
                print(line.rstrip())

    # Latest date sample
    print(f"\n--- Latest date from {category_output} ---")
    with open(category_output, 'r') as f:
        lines = f.readlines()
        for line in lines[-15:]:
            print(line.rstrip())

def main():
    """Main execution function."""
    start_time = datetime.now()

    print("="*60)
    print("NBS BASE DATA AGGREGATION")
    print("="*60)
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Check if NBS_BASE directory exists
    if not os.path.exists(NBS_BASE_DIR):
        print(f"ERROR: Directory '{NBS_BASE_DIR}' not found!")
        return

    # Process all files
    service_data, category_data = process_files()

    # Write output files
    write_service_output(service_data, SERVICE_OUTPUT)
    write_category_output(category_data, CATEGORY_OUTPUT)

    # Show summary
    show_summary(SERVICE_OUTPUT, CATEGORY_OUTPUT)

    # Execution time
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    print(f"\nExecution time: {duration:.2f} seconds")
    print("="*60)

if __name__ == "__main__":
    main()
