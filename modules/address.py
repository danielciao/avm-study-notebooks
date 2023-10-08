import re

import pandas as pd


def str_concat(*args):
    filtered = filter(lambda x: pd.notna(x), args)
    mapped = map(lambda x: x.replace(',', '').strip().lower(), filtered)
    return ', '.join(mapped)


def extract_flat_number(address):
    flat_pattern = re.compile(r'\bFlat\s+([A-Za-z0-9]+)', re.IGNORECASE)
    flat_match = flat_pattern.search(address)
    return flat_match.group() if flat_match else None


def extract_flat_name(address):
    number_pattern = re.compile(
        r'(Flat\s+\d+,?\s+)?([A-Za-z].+)?', re.IGNORECASE)
    number_match = number_pattern.search(address)
    return number_match.group(2) if number_match else None


def extract_number(address):
    number_pattern = re.compile(r'^(\d+\w*)', re.IGNORECASE)
    number_match = number_pattern.search(address)
    return number_match.group(1) if number_match else None


def extract_name(address):
    name_pattern = re.compile(r'(\d+\w*,?\s+)?([A-Za-z].+)?')
    name_match = name_pattern.search(address)
    return name_match.group(2) if name_match else None


def get_age_band(age):
    if (age <= 5):
        return 5
    elif (age <= 10):
        return 10
    elif (age <= 20):
        return 20
    elif (age <= 30):
        return 30
    elif (age <= 40):
        return 40
    elif (age <= 50):
        return 50
    elif (age <= 60):
        return 60
    elif (age <= 70):
        return 70
    elif (age <= 80):
        return 80
    elif (age <= 90):
        return 90
    else:
        return 100
