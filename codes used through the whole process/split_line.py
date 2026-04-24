import os
import argparse
from tqdm import tqdm

def split_long_lines(file_path, max_length=128):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    new_lines = []
    for line in lines:
        line = line.rstrip('\n')
        while len(line) > max_length:
            split_pos = max_length
            # Look for the last space or line break to ensure a complete word or number
            while split_pos > 0 and not line[split_pos].isspace():
                split_pos -= 1
            if split_pos == 0:
                split_pos = max_length
            new_lines.append(line[:split_pos].rstrip() + '\n')
            line = line[split_pos:].lstrip()
        new_lines.append(line + '\n')

    with open(file_path, 'w', encoding='utf-8') as file:
        file.writelines(new_lines)

def process_directory(directory, max_length=128):
    files = [f for f in os.listdir(directory) if f.endswith('.txt')]
    with tqdm(total=len(files), desc="Processing Files") as pbar:
        for filename in files:
            file_path = os.path.join(directory, filename)
            split_long_lines(file_path, max_length)
            pbar.update(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process TXT files to ensure each line is less than a specified number of characters.')
    parser.add_argument('directory', type=str, help='The directory containing TXT files.')
    parser.add_argument('--max_length', type=int, default=128, help='The maximum length of each line.')
    args = parser.parse_args()

    process_directory(args.directory, args.max_length)
