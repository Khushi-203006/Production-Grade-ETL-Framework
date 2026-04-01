# ===============================================================================================================
# Question 20: File Watcher (auto-detect new files)
# ===============================================================================================================

import time
import os

def watch_folder(folder_path: str):

    print(f"Watching folder: {folder_path}")

    # Already existing files
    seen_files = set(os.listdir(folder_path))

    try:
        while True:
            time.sleep(5)  # check every 5 seconds

            current_files = set(os.listdir(folder_path))

            # detect new files
            new_files = current_files - seen_files

            for file in new_files:
                print(f"New file detected: {file}")

                process_file(os.path.join(folder_path, file))

            seen_files = current_files

    except KeyboardInterrupt:
        print("\n🛑 Stopping file watcher safely!")


def process_file(file_path: str):
    print(f"Processing file: {file_path}")


if __name__ == "__main__":

    folder_path = "../data/raw"

    watch_folder(folder_path)