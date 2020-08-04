import re


def get_date_from_file_name(f_name):
    tokens = f_name.split(".")
    date = tokens[0][-6:]
    month = date[0:2]
    day = date[2:4]
    year = "20{}".format(date[4:6])
    new_date_str = "{}-{}-{}".format(year, month, day)
    return new_date_str


def extract_address(text):
    ret = []
    lines = text.split('\n')
    for i in range(len(lines)):
        # don't process the title line
        if i == 0:
            continue
        tokens = re.split(" on | at |\. \.| , |, ", lines[i])
        if len(tokens) > 1:
            tokens = tokens[1:-1]
            n = len(tokens)
            cities = ["San Bruno", "So. San Francisco", "Daly City", "Brisbane", "Pacifica"]
            if n > 0 and tokens[n - 1] not in cities:
                tokens.append("So. San Francisco")

            tokens.append("CA, 94080")
            address = ", ".join(tokens)
            address = address.replace('/', " and ")

            ret.append(address)
    return ret