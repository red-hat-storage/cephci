import concurrent.futures
import os
import sys
import time


# Function to simulate writing data to a file
def write_to_file(thread_id, file_path):
    while True:
        try:
            with open(file_path, "a") as f:
                f.write(f"Thread {thread_id} writing data...\n")
            time.sleep(1)  # Simulate some work with a sleep
            break
        except Exception as e:
            print(f"Thread {thread_id} failed to access file: {e}")
            time.sleep(5)  # Wait for 5 seconds before retrying


def main(directory_path):
    # Ensure the directory exists
    os.makedirs(directory_path, exist_ok=True)

    # Create an empty file to write to
    file_path = os.path.join(directory_path, "shared_file.txt")
    with open(file_path, "w") as f:
        f.read()
        pass

    # Number of threads
    NUM_THREADS = 100
    MAX_WORKERS = 100  # Number of concurrent threads

    # Using ThreadPoolExecutor to manage threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(write_to_file, i, file_path)
            for i in range(1, NUM_THREADS + 1)
        ]
        concurrent.futures.wait(futures)

    print("All threads have finished writing.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <directory_path>")
        sys.exit(1)

    directory_path = sys.argv[1]
    main(directory_path)
