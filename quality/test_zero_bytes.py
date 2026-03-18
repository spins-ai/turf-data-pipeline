#!/usr/bin/env python3
"""
Test for 0-byte files in output directory.
Finds all empty (0-byte) files which likely indicate failed writes or truncation.
"""
import argparse
import os
import sys


def find_zero_byte_files(output_dir):
    """Walk directory and find all 0-byte files."""
    zero_files = []
    all_files = 0
    for root, _dirs, filenames in os.walk(output_dir):
        for fname in sorted(filenames):
            filepath = os.path.join(root, fname)
            all_files += 1
            try:
                size = os.path.getsize(filepath)
                if size == 0:
                    zero_files.append(os.path.relpath(filepath, output_dir))
            except OSError:
                pass
    return zero_files, all_files


def main():
    parser = argparse.ArgumentParser(description="Find 0-byte files in output directory")
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=None,
        help="Directory to scan (default: ../output or backup_20260314)",
    )
    args = parser.parse_args()

    # Resolve output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)
        candidate_output = os.path.join(parent_dir, "output")
        candidate_backup = os.path.join(parent_dir, "backup_20260314")
        if os.path.isdir(candidate_output):
            output_dir = candidate_output
        elif os.path.isdir(candidate_backup):
            output_dir = candidate_backup
        else:
            print(f"FAIL: No output directory found (tried {candidate_output}, {candidate_backup})")
            sys.exit(1)

    print(f"=== Zero-Byte File Test ===")
    print(f"Scanning: {output_dir}\n")

    zero_files, total_files = find_zero_byte_files(output_dir)

    if not zero_files:
        print(f"  PASS  No 0-byte files found ({total_files} files scanned)")
        print(f"\n--- Summary ---")
        print(f"Total files scanned: {total_files}")
        print(f"Zero-byte files: 0")
        print(f"\nOverall: PASS")
        return 0
    else:
        print(f"  FAIL  Found {len(zero_files)} zero-byte file(s):\n")
        for zf in zero_files:
            print(f"        - {zf}")
        print(f"\n--- Summary ---")
        print(f"Total files scanned: {total_files}")
        print(f"Zero-byte files: {len(zero_files)}")
        print(f"\nOverall: FAIL")
        return 1


if __name__ == "__main__":
    sys.exit(main())
