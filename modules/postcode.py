from postcodes_uk import Postcode


def standardise_postcode(postcode):
    try:
        code = Postcode.from_string(postcode.upper())
        return f'{code.outward_code} {code.inward_code}'
    except:
        print(f'Cannot parse {postcode}')
        return None
