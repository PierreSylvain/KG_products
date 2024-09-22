from src.modules.split_glued_words import split_glued_words


def parse_specifications(spec_str: str) -> dict:
    if not spec_str:
        return {}
    specs = spec_str.split('|')
    spec_dict = {}
    for spec in specs:
        # Don't handle cases where there is no colon
        if ':' in spec:
            # Special case for ASIN keep it as is
            if spec.startswith('ASIN'):
                spec_dict['ASIN'] = spec.split(':', 1)[1].strip()
                continue
            key, value = spec.split(':', 1)

            new_key = split_glued_words(key.strip())
            # Hack to handle cases where the new_key contains "no glued words"
            if 'no glued words' in new_key:
                new_key = key.strip()

            new_value = split_glued_words(value.strip())
            # Hack to handle cases where the new_value contains "no glued words"
            if 'no glued words' in new_value:
                new_value = value.strip()

            spec_dict[new_key] = new_value

    return spec_dict





