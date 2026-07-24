import argparse
import os
import random


def get_random_offsets(file_path, count, max_len=1):
    file_size = os.path.getsize(file_path)
    return [random.randint(0, max(file_size - max_len, 0)) for _ in range(count)]


def read_bytes_from_file(file_path, offsets, length):
    try:
        with open(file_path, "rb") as f:
            for offset in offsets:
                f.seek(offset)
                data = f.read(length)
                print(f"[READ] {file_path} @ Offset {offset}: {data}")
    except Exception as e:
        print(f"[READ ERROR] {file_path}: {e}")


def write_bytes_to_file(file_path, offsets, new_bytes):
    try:
        with open(file_path, "r+b") as f:
            for offset in offsets:
                f.seek(offset)
                f.write(new_bytes)
                print(f"[WRITE] {file_path} @ Offset {offset}: Wrote {new_bytes}")
    except Exception as e:
        print(f"[WRITE ERROR] {file_path}: {e}")


def remove_bytes_from_file(file_path, offsets, length):
    try:
        with open(file_path, "rb") as f:
            content = bytearray(f.read())

        # Sort offsets in reverse so index shifts don’t affect subsequent deletions
        for offset in sorted(offsets, reverse=True):
            if offset + length <= len(content):
                del content[offset : offset + length]
                print(f"[REMOVE] {file_path} @ Offset {offset}: Removed {length} bytes")

        with open(file_path, "wb") as f:
            f.write(content)

    except Exception as e:
        print(f"[REMOVE ERROR] {file_path}: {e}")


def get_random_files_from_directory(directory, file_count):
    files = [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, f))
    ]
    return random.sample(files, min(file_count, len(files)))


def main():
    parser = argparse.ArgumentParser(
        description="Randomly read, write, or remove bytes in multiple files."
    )
    parser.add_argument("directory", help="Path to the directory containing files")
    parser.add_argument(
        "--file-count", type=int, default=1, help="Number of files to operate on"
    )
    parser.add_argument(
        "--mode",
        choices=["read", "write", "remove"],
        required=True,
        help="Operation mode",
    )
    parser.add_argument("--bytes", help="Bytes to write (required for write mode)")
    parser.add_argument(
        "--length",
        type=int,
        default=1,
        help="Number of bytes to read/write/remove per offset",
    )

    args = parser.parse_args()

    if args.mode == "write" and not args.bytes:
        print("Error: --bytes is required in write mode.")
        return

    selected_files = get_random_files_from_directory(args.directory, args.file_count)

    for file_path in selected_files:
        offsets = get_random_offsets(file_path, 10, max_len=args.length)
        if args.mode == "read":
            read_bytes_from_file(file_path, offsets, args.length)
        elif args.mode == "write":
            write_bytes_to_file(file_path, offsets, args.bytes.encode()[: args.length])
        elif args.mode == "remove":
            remove_bytes_from_file(file_path, offsets, args.length)


if __name__ == "__main__":
    main()
