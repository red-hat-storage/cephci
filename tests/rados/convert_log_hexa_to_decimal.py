import os
import re
import sys


def convert_hex_to_dec(match):
    hex_str = match.group(0)
    try:
        return str(int(hex_str, 16))
    except ValueError:
        return hex_str


def convert_file_create_new(filename):
    if not os.path.isfile(filename):
        print(f" Error: File '{filename}' does not exist.")
        return

    with open(filename, "r") as f:
        content = f.read()

    # Replace hex-like values (with at least one letter a-f to avoid matching plain decimal numbers)
    converted = re.sub(r"\b(?:0x)?[a-fA-F0-9]{3,}\b", convert_hex_to_dec, content)

    # Construct output filename
    name, ext = os.path.splitext(filename)
    output_filename = f"{name}_converted{ext}"

    with open(output_filename, "w") as f:
        f.write(converted)

    print(f"✔ Hexadecimal values converted. Output saved to '{output_filename}'.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python convert_hex.py <log_file>")
    else:
        convert_file_create_new(sys.argv[1])
