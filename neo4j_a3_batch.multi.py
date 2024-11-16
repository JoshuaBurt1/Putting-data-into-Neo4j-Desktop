import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from neo4j import GraphDatabase
import csv
import time
import random

# Neo4j connection parameters
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "randompw001")

# To track processed sub-arrays and keep track of errors
processed_subarrays = set()
error_count = 0

current_threads = 32  # Start with 64 threads

def main():
    global current_threads, error_count
    df = pd.read_csv('webcrawl.csv', sep='\t', on_bad_lines='skip')  # Use '\t' as the separator 
    
    # Node creation subsets:
    unique_targets = df['target'].unique().tolist()
    unique_urls = df['url'].unique().tolist()
    all_urls = list(set(unique_urls) | set(unique_targets))
    print(f"Number of unique links: {len(all_urls)}")

    # URL-target pairs with multiple links to each other (THIS NEEDS TO BE ADDED USING CREATE LINK AFTER FINAL STAGE) 
    pair_counts = df.groupby(['url', 'target']).size().reset_index(name='count')
    duplicates = pair_counts[pair_counts['count'] > 1]
    duplicates_sorted = duplicates.sort_values(by='count', ascending=True)
    duplicates_sorted['count'] = duplicates_sorted['count'] - 1
    print(f"Number of duplicate links: {len(duplicates_sorted)}")
    
    # An array with subarrays of [(url)[target]] pairs
    url_target_dict = df.groupby('url')['target'].apply(list).to_dict()
    url_target_subarrays = [[url, targets] for url, targets in url_target_dict.items()]
    url_target_subarrays = sorted(url_target_subarrays, key=lambda x: len(x[1]), reverse=True)  # largest first
    print(f"Total subarrays (1:many relationships): {len(url_target_subarrays)}")

    # Step 1: Split into 1000 sub-lists for processing
    num_chunks = 500
    chunk_size = len(url_target_subarrays) // num_chunks
    chunks = [url_target_subarrays[i:i + chunk_size] for i in range(0, len(url_target_subarrays), chunk_size)]
    total_chunks = len(chunks)
    if total_chunks < num_chunks:
        chunks.extend([[] for _ in range(num_chunks - total_chunks)])  # Pad with empty chunks if necessary

    print(f"Total chunks: {total_chunks}")

    # Collect Unique Targets before Phase A
    unique_targets_set = set()
    for _, targets in url_target_subarrays:
        unique_targets_set.update(targets)
    
    # Step 2: Write the chunk statistics (URLs count and Targets count) to a CSV file before Phase A
    with open('chunk_statistics.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Chunk Number', 'URLs Count', 'Targets Count', 'Unique URLs Count', 'Unique Targets Count'])  # CSV header

        # Process each chunk
        for i, chunk in enumerate(chunks, start=1):
            # Count the total number of URLs and targets in this chunk
            num_urls = len(chunk)  # One URL per sub-array
            num_targets = sum(len(targets) for _, targets in chunk)  # Sum of all target lists in the chunk
            
            # Track unique URLs and unique targets in the chunk
            unique_urls_in_chunk = set(url for url, _ in chunk)  # Collect unique URLs in this chunk
            unique_targets_in_chunk = set(target for _, targets in chunk for target in targets)  # Collect unique targets in this chunk

            # Write the chunk statistics (URLs + Targets count) to the CSV file
            writer.writerow([i, num_urls, num_targets, len(unique_urls_in_chunk), len(unique_targets_in_chunk)])
            
    # After processing all chunks, read the CSV to calculate the total unique URLs and targets
    total_targets = 0
    total_unique_urls = 0
    total_unique_targets = 0

    # Read the chunk statistics from the CSV file
    with open('chunk_statistics.csv', mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        
        # Process each row in the CSV to accumulate the unique counts
        for row in reader:
            total_targets += int(row[2])  # Add the Unique URLs count (4th column)
            total_unique_urls += int(row[3])  # Add the Unique URLs count (4th column)
            total_unique_targets += int(row[4])  # Add the Unique Targets count (5th column)

    # Print the total sum of unique URLs and unique targets across all chunks
    print("\nchunk_statistics.csv values")
    print(f"Total unique URLs across all chunks: {total_unique_urls}")
    print(f"Total targets across all chunks: {total_targets}")
    print(f"Total unique targets across all chunks: {total_unique_targets}")
    print(f"Chunk statistics written to 'chunk_statistics.csv'.")

    # Step 2: Process chunks in stages (first URL node creation, then relationships)
    # this is necessary to prevent: 
    # 1. duplicate nodes - use async or regular creation -> duplication is a big problem, needs to be exact #
    # 2. duplicate relationships - multithread this -> duplicates should be fixable

    # Function to create the index for 'url' property on 'Page' nodes
    def create_url_index(driver):
        with driver.session() as session:
            session.run("CREATE INDEX FOR (p:Page) ON (p.url)")
            print("Index created on :Page(url)")

    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        start_time = time.perf_counter()

        #create_url_index(driver)

        # Open the CSV file to record iteration durations
        with open('iteration_durations.csv', mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Iteration', 'Duration (Seconds)'])  # CSV header

            for i in range(total_chunks):
                chunk = chunks[i]

                # Start time for this iteration
                iteration_start_time = time.perf_counter()

                # Collect all the URLs and targets for this chunk into a single batch
                nodes_to_create = []

                # Collect URLs and Targets in bulk
                for url, targets in chunk:
                    # Add URL to the list
                    nodes_to_create.append({"type": "url", "value": url})
                    # Add all Targets to the list
                    for target in targets:
                        nodes_to_create.append({"type": "target", "value": target})

                # Count the number of URLs and Targets in the chunk
                num_urls = sum(1 for node in nodes_to_create if node["type"] == "url")
                num_targets = sum(1 for node in nodes_to_create if node["type"] == "target")

                try:
                    # Phase A: Batch create URL and Target nodes for the chunk
                    with driver.session() as session:
                        query = """
                        UNWIND $nodes AS node
                        MERGE (n:Page {url: node.value})
                        """
                        session.execute_write(lambda tx: tx.run(query, nodes=nodes_to_create))

                    # Print the number of URLs and Targets created in Phase A for this chunk
                    print(f"Phase A: {num_urls} URLs and {num_targets} Targets created in chunk {i + 1}/{total_chunks}.")

                except Exception as e:
                    print(f"Error creating nodes for chunk {i + 1}/{total_chunks}: {e}")

                # Phase B: Create relationships for the chunk
                with ThreadPoolExecutor(max_workers=current_threads) as executor:
                    futures = [executor.submit(execute_write, driver, url_target_subarray) for url_target_subarray in chunk]
                    for future in futures:
                        try:
                            future.result()  # Wait for the task to complete and check for exceptions
                        except Exception as e:
                            print(f"Error encountered while processing a Target: {e}")

                print(f"Phase B: Relationships for chunk {i + 1}/{total_chunks} created")
                # End time for this iteration
                iteration_end_time = time.perf_counter()
                iteration_duration = iteration_end_time - iteration_start_time  # Calculate the duration
                # Write the iteration number and duration to the CSV file
                writer.writerow([i + 1, iteration_duration])  # +1 for human-readable iteration numbering
        
        # Step 3: Add duplicate relationships
        create_duplicate_relationships(driver, duplicates_sorted)
        # Measure total execution time
        end_time = time.perf_counter()
        total_duration = end_time - start_time
        print(f"\nTotal loop time: {total_duration:.4f} seconds.")

def execute_write(driver, url_target_subarray):
    url, targets = url_target_subarray
    subarray_tuple = (url, tuple(targets))

    if subarray_tuple not in processed_subarrays:
        attempt = 1
        while True:
            try:
                with driver.session(database="neo4j") as session:
                    session.execute_write(create_link_bulk_tx, url, targets)
                    processed_subarrays.add(subarray_tuple)
                    return  # Task succeeded, exit

            except Exception as e:
                #print(f"Error executing queries for subarray: {url_target_subarray} -> {e}")
                print(f"Error: {e}")
                # Exponential backoff with jitter
                backoff_time = min(2 ** attempt + random.uniform(0, 1), 60)
                print(f"Retrying after {backoff_time:.2f} seconds...")
                time.sleep(backoff_time)  # Wait before retrying
                attempt += 1  # Increment attempt counter for next retry
    else:
        print(f"Skipping already processed sub-array: {url_target_subarray}")


def create_link_bulk_tx(tx, pgurl, targets):
    try:
        # Query to MERGE pages and their links
        query = """
        UNWIND $pairs AS pair
        MERGE (a:Page {url: pair[0]})
        MERGE (b:Page {url: pair[1]})
        MERGE (a)-[l:LINKS {weight: 1}]->(b)
        """
        # Prepare the list of URL-target pairs to be passed to UNWIND
        pairs = [[pgurl, target] for target in targets]
        tx.run(query, pairs=pairs)  # Run the query with the prepared pairs
    except Exception as e:
        print(f"Transaction failed for {pgurl} -> {targets}: {e}")

def create_duplicate_relationships(driver, duplicates_sorted):
    query = """
    UNWIND $duplicates AS duplicate
    MATCH (u:Page {url: duplicate.url})
    MATCH (t:Page {url: duplicate.target})
    WITH u, t, duplicate.count AS duplicate_count
    UNWIND range(1, duplicate_count) AS n  // Create 'count' relationships
    CREATE (u)-[:LINKS {weight: 1}]->(t)
    """

    with driver.session() as session:
        session.run(query, duplicates=duplicates_sorted.to_dict('records'))
        print("Duplicate relationships created based on count.")

if __name__ == "__main__":
    main()
