import re
import json
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import OrderedDict
from tqdm import tqdm
import glob
import os

# Define a function to parse a ternary
def parse_triple(triple):
    pattern = re.compile(r"\[entity1, ([^\]]+)\]: (.+?), \[relation, ([^\]]+)\]: (.+?), \[entity2, ([^\]]+)\]: (.+)")
    match = pattern.search(triple["triples"])
    if match:
        entity1_id, entity1, _, _, entity2_id, entity2 = match.groups()
        return (entity1_id, entity1.strip()), (entity2_id, entity2.strip())
    else:
        raise ValueError("Triple format is incorrect")

# Get Wikipedia page titles in bulk
def batch_fetch_titles(entity_ids, title_cache):
    url = 'https://www.wikidata.org/w/api.php'
    batch_size = 50  # Number of entities per request

    def fetch_batch(batch):
        params = {
            'action': 'wbgetentities',
            'ids': '|'.join(batch),
            'format': 'json',
            'props': 'sitelinks'
        }
        max_retries = 5
        backoff_time = 1  # Initial wait time is 1 second

        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                for entity_id in batch:
                    sitelinks = data['entities'].get(entity_id, {}).get('sitelinks', {})
                    title = sitelinks.get('enwiki', {}).get('title', None)
                    title_cache[entity_id] = title
                break
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    time.sleep(backoff_time)
                    backoff_time *= 2  # Double the wait time next time
                else:
                    raise e  # Non-429 errors are thrown
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                time.sleep(backoff_time)
                backoff_time *= 2

    with ThreadPoolExecutor(max_workers=200) as executor:  
        futures = [executor.submit(fetch_batch, entity_ids[i:i+batch_size]) for i in range(0, len(entity_ids), batch_size)]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching Wikipedia titles"):
            future.result()  

# Get Wikipedia page content
def get_wikipedia_content(title, retries=3, delay=5):
    url = f"https://en.wikipedia.org/w/api.php?action=query&prop=extracts&exintro=&explaintext=&format=json&titles={title}"
    for _ in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            page = next(iter(data['query']['pages'].values()))
            if 'extract' in page:
                return page['extract']
            else:
                return None
        except requests.RequestException as e:
            print(f"Error fetching Wikipedia content for {title}: {e}")
            time.sleep(delay)
    return None

# Handling of individual entities
def process_entity(entity, title_cache):
    entity_id, entity_name = entity
    title = title_cache.get(entity_id, None)
    if title:
        content = get_wikipedia_content(title)
        if not content:  # If no content is found, it may be a redirection
            content = handle_redirects(title)
        if content:
            return f"[{entity_name}, {entity_id}]: {content}\n"
        else:
            return f"[{entity_name}, {entity_id}]: No content found.\n"
    else:
        return f"[{entity_name}, {entity_id}]: No Wikipedia title found.\n"

# Functions that handle redirects
def handle_redirects(title, retries=3, delay=5):
    url = f"https://en.wikipedia.org/w/api.php?action=query&prop=extracts&exintro=&explaintext=&redirects=1&format=json&titles={title}"
    for _ in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            page = next(iter(data['query']['pages'].values()))
            if 'extract' in page:
                return page['extract']
            else:
                return None
        except requests.RequestException as e:
            print(f"Error handling redirects for {title}: {e}")
            time.sleep(delay)
    return None

# Read processed file records
def read_processed_files(record_file):
    if os.path.exists(record_file):
        with open(record_file, 'r') as f:
            return set(line.strip() for line in f)
    return set()

# Write processed file records
def write_processed_file(record_file, filename):
    with open(record_file, 'a') as f:
        f.write(filename + '\n')

input_files_pattern = '/hkfs/home/project/hk-project-p00201316/st_st176945/entity_rels_reformed/*_reformed.jsonl'
output_dir = '/hkfs/home/project/hk-project-p00201316/st_st176945/paragraph111/'
processed_files_record = '/hkfs/home/project/hk-project-p00201316/st_st176945/processed_files.txt'

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

processed_files = read_processed_files(processed_files_record)
input_files = glob.glob(input_files_pattern)

for input_file in input_files:
    if input_file in processed_files:
        print(f"Skipping already processed file: {input_file}")
        continue

    entities = OrderedDict()
    output_file = os.path.join(output_dir, os.path.basename(input_file).replace('_reformed.jsonl', '_paragraph.txt'))

    # Step 1: Extract entity information and record order
    with open(input_file, 'r', encoding='utf-8') as infile:
        for line in infile:
            triple = json.loads(line.strip())
            try:
                entity1, entity2 = parse_triple(triple)
                if entity1 not in entities:
                    entities[entity1] = None
                if entity2 not in entities:
                    entities[entity2] = None
            except ValueError as e:
                print(f"Error parsing line: {line}")
                print(e)

    # Step 2: Bulk Fetch Wikipedia Page Titles
    entity_ids = [entity[0] for entity in entities]
    title_cache = {}
    batch_fetch_titles(entity_ids, title_cache)

    # Step 3: Use multithreading to process Wikipedia requests and introduce a progress bar
    max_workers = 15 

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_entity, entity, title_cache): entity for entity in entities}
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Processing entities in {input_file}"):
            entity = futures[future]
            try:
                result = future.result()
                entities[entity] = result
            except Exception as e:
                print(f"Error processing entity {entity}: {e}")

    # Write the results to the output file in the order of input
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for entity, result in entities.items():
            outfile.write(result)

    # Recording of processed documents
    write_processed_file(processed_files_record, input_file)