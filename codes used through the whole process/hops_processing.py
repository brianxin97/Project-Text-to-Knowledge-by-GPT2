import os
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

def extract_entities_labels(line):
    try:
        triples_part = line.split('<|triple|>')[1].split('<|endoftriple|>')[0]
        entity_parts = triples_part.split('[')
        entity_labels = []
        for entity_part in entity_parts:
            if 'entity' in entity_part:
                label = entity_part.split(']:')[1].split(',')[0].strip().rstrip('}')
                entity_labels.append(label)
        return entity_labels[:2]  # Return only the first two entity labels
    except IndexError:
        return ["Unknown", "Unknown"]

def find_longest_matching_line(entity, hops_data):
    matching_lines = [entry['hop'] for entry in hops_data if f'{entity}' in entry['hop']]
    if matching_lines:
        return max(matching_lines, key=len)
    return None

def process_files(merged_file, hops_file):
    # Load merged.txt file
    with open(merged_file, 'r') as file:
        merged_data = file.readlines()
    
    # Extract entity labels from merged.txt
    entity_labels = [extract_entities_labels(line) for line in merged_data]
    
    # Load hops.jsonl file
    with open(hops_file, 'r') as file:
        hops_data = [json.loads(line) for line in file]
    
    # Append the longest line from hops file to the corresponding lines in merged file
    for i, labels in enumerate(entity_labels):
        if labels[0] != "Unknown" and labels[1] != "Unknown":
            line1_content = find_longest_matching_line(labels[0], hops_data)
            line2_content = find_longest_matching_line(labels[1], hops_data)
            if line1_content and line2_content:
                longer_line = line1_content if len(line1_content) > len(line2_content) else line2_content
                merged_data[i] = merged_data[i].strip() + ' ' + longer_line + '\n'
    
    return merged_data

def save_results(merged_file, hops_file):
    merged_file_path = os.path.join(merged_files_dir, merged_file)
    hops_file_path = os.path.join(hops_files_dir, hops_file)
    
    result_data = process_files(merged_file_path, hops_file_path)
    
    # Save the result to a new file in the output directory
    output_file = merged_file.replace('merged', 'output')
    with open(os.path.join(output_dir, output_file), 'w') as file:
        file.writelines(result_data)

# Directory containing the files
merged_files_dir = "newtext2kg"
hops_files_dir = "generated_hops"
output_dir = "0"

# Create output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Get list of files
merged_files = sorted([f for f in os.listdir(merged_files_dir) if f.startswith('merged') and f.endswith('.txt')])
hops_files = sorted([f for f in os.listdir(hops_files_dir) if f.endswith('.jsonl') and '__hops' in f])

# Pair files based on numbering
file_pairs = []
for merged_file in merged_files:
    number = merged_file.split('_')[1].split('.')[0]  # Extract the number
    hops_file = f"{number}__hops.jsonl"
    if hops_file in hops_files:
        file_pairs.append((merged_file, hops_file))

# Define the number of threads
num_threads = 152

# Process each pair of files with a progress bar and multithreading
with ThreadPoolExecutor(max_workers=num_threads) as executor:
    list(tqdm(executor.map(lambda pair: save_results(pair[0], pair[1]), file_pairs), total=len(file_pairs), desc="Processing files"))

print("Batch processing completed.")
