import sys


def create_large_file(file_paths):
    file_size = 1024 * 1024 * 100000

    for file_path in file_paths:
        with open(file_path, "wb") as file:
            chunk_size = 1
            num_chunks = file_size // chunk_size
            for _ in range(num_chunks):
                chunk = b"\0" * chunk_size
                file.write(chunk)
        print(f"File '{file_path}' created with a size of {file_size} bytes.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python create_large_file.py <file_paths>")
        sys.exit(1)

    file_paths = sys.argv[1:]
    create_large_file(file_paths)
