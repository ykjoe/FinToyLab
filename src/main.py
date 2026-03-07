#!/usr/bin/env python3
"""
Main entry point for FinCapLab
"""
import sys
import os

# ensure `src` directory is on sys.path so that `common` package is importable
sys.path.insert(0, os.path.dirname(__file__))

from common.db import init_db

def main():
    init_db()
    # Add your main logic here
    print("FinCapLab is running!")

if __name__ == "__main__":
    main()