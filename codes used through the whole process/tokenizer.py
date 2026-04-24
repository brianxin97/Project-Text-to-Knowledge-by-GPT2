import os
from tqdm import tqdm
import numpy as np
import tiktoken
from concurrent.futures import ThreadPoolExecutor, as_completed

# Number of workers in .map() call
num_proc = 76

enc = tiktoken.get_encoding("gpt2")

def load_local_dataset(data_dir):
    """
    Load text files from a local directory and return a list of file paths.
    Each file will be processed one by one.
    """
    dataset = []
    for filename in os.listdir(data_dir):
        if filename.endswith(".txt"):
            dataset.append(os.path.join(data_dir, filename))
    return dataset

def process_file(filepath):
    """
    Process a single file: read the content, encode, and return token ids.
    """
    with open(filepath, 'r', encoding='utf-8') as file:
        text = file.read()
    ids = enc.encode_ordinary(text)
    ids.append(enc.eot_token)
    return {'ids': ids, 'len': len(ids)}

def write_to_bin_file(filename, data_generator, dtype):
    """
    Write data to a binary file in chunks to avoid memory overflow.
    """
    total_len = 0
    with open(filename, 'wb') as f:
        for data in data_generator:
            ids = np.array(data['ids'], dtype=dtype)
            total_len += data['len']
            ids.tofile(f)
    return total_len

if __name__ == '__main__':
    data_dir = 'final'  
    dataset = load_local_dataset(data_dir)

    np.random.seed(2357)
    np.random.shuffle(dataset)
    split_index = int(len(dataset) * 0.9995)
    train_files = dataset[:split_index]
    val_files = dataset[split_index:]

    splits = {'train': train_files, 'val': val_files}

    # Dictionary to store token counts
    token_counts = {}

    # Process each split separately
    for split, files in splits.items():
        filename = os.path.join(os.path.dirname(__file__), f'{split}.bin')
        dtype = np.uint16  # (can do since enc.max_token_value == 50256 is < 2**16)

        with ThreadPoolExecutor(max_workers=num_proc) as executor:
            future_to_file = {executor.submit(process_file, file_path): file_path for file_path in files}
            data_generator = (future.result() for future in tqdm(as_completed(future_to_file), total=len(files), desc=f'processing {split} split'))
            
            # Write data to bin file in chunks
            token_counts[split] = write_to_bin_file(filename, data_generator, dtype)

    # Write token counts to a text file
    with open("token_counts.txt", "w") as f:
        for split, count in token_counts.items():
            f.write(f"{split} token count: {count}\n")

    print("Token counts have been written to token_counts.txt")

