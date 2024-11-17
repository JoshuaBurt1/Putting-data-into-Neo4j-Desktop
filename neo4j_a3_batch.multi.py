import pandas as pd
import time
import csv
from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# Neo4j connection parameters
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "randompw001")

# Retry settings
MAX_RETRIES = 10
RETRY_DELAY = 0.5  # seconds

#UNIQUE CONSTRAINT is necessary of multithreading
def create_url_index(driver):
    with driver.session() as session:
        # Step 1: Drop the existing index on :Page(url) if it exists
        try:
            result = session.run("SHOW INDEXES YIELD name, labelsOrTypes, properties")
            indexes = list(result)  
            print("Indexes in the database:")
            for index in indexes:
                print(index)
            for index in indexes:
                # Check if labelsOrTypes is not None and contains 'Page', and properties contains 'url'
                if index['labelsOrTypes'] and 'Page' in index['labelsOrTypes'] and index['properties'] and 'url' in index['properties']:
                    print(f"Attempting to DROP INDEX {index['name']}")
                    session.run(f"DROP INDEX {index['name']}")
                    print(f"Existing index {index['name']} on :Page(url) dropped.")
                    break
            else:
                print("No existing index on :Page(url) found.")
        except Exception as e:
            print(f"Error in checking/dropping index: {e}")
        # Step 2: Create the unique constraint on :Page(url)
        try:
            session.run("CREATE CONSTRAINT FOR (p:Page) REQUIRE p.url IS UNIQUE")
            print("Unique constraint created on :Page(url)")
        except Exception as e:
            print(f"Error creating unique constraint: {e}")

def create_nodes_in_batch(driver, nodes_to_create):
    """Helper function to create nodes in a batch using MERGE to avoid duplicates"""
    with driver.session() as session:
        query = """
        UNWIND $nodes AS node
        MERGE (n:Page {url: node.value})
        """
        session.execute_write(lambda tx: tx.run(query, nodes=nodes_to_create))

def create_relationships(driver, chunk):
    """Helper function to create relationships between nodes in a batch"""
    with driver.session() as session:
        query = """
        UNWIND $pairs AS pair
        MERGE (a:Page {url: pair[0]})
        MERGE (b:Page {url: pair[1]})
        MERGE (a)-[:LINKS {weight: 1}]->(b)
        """
        pairs = [[url, target] for url, targets in chunk for target in targets]
        session.execute_write(lambda tx: tx.run(query, pairs=pairs))

def retry_operation(operation, *args, **kwargs):
    """Retry a function in case of failure"""
    last_exception = None
    for attempt in range(MAX_RETRIES):
        try:
            operation(*args, **kwargs)
            return  # Success, exit the function
        except Exception as e:
            last_exception = e
            print(f"Error during attempt {attempt + 1}/{MAX_RETRIES}: {e}")
            time.sleep(RETRY_DELAY + random.uniform(0, 2))  # Randomize the delay a bit to avoid collisions
    print(f"Operation failed after {MAX_RETRIES} attempts: {last_exception}")
    raise last_exception  # Raise the last exception encountered after retries are exhausted

def track_duration(func, *args, **kwargs):
    """Wrapper to track iteration durations for each task in the thread pool"""
    start_time = time.perf_counter()  # Record start time
    try:
        func(*args, **kwargs)
    except Exception as e:
        print(f"Error executing function {func.__name__}: {e}")
        raise e
    end_time = time.perf_counter()  # Record end time
    duration = end_time - start_time
    return duration  # Return duration to be logged

def create_balanced_chunks(url_target_subarrays, num_chunks):
    total_nodes = sum(len(targets) + 1 for _, targets in url_target_subarrays)  # 1 for each URL + targets
    nodes_per_chunk = total_nodes // num_chunks
    chunks = []
    current_chunk = []
    current_chunk_size = 0

    for url, targets in url_target_subarrays:
        node_size = len(targets) + 1  # 1 for the URL itself
        current_chunk_size += node_size
        current_chunk.append([url, targets])

        # If adding this URL and its targets exceeds the max chunk size, finalize the current chunk
        if current_chunk_size >= nodes_per_chunk:
            chunks.append(current_chunk)
            current_chunk = []
            current_chunk_size = 0

    # If any items remain, add them to the last chunk
    if current_chunk:
        chunks.append(current_chunk)

    return chunks

def main():
    # Start total loop time right at the beginning of the program
    start_time = time.perf_counter()

    df = pd.read_csv('sample.csv', sep='\t', on_bad_lines='skip')  # Use '\t' as separator

   # Optimized calculation of unique URLs and targets
    all_urls = pd.concat([df['url'], df['target']]).unique()
    print(f"Number of unique links: {len(all_urls)}")

    # Optimized calculation for duplicate links
    pair_counts = df[df.duplicated(subset=['url', 'target'], keep=False)] \
        .groupby(['url', 'target']).size().reset_index(name='count')
    duplicates_sorted = pair_counts[pair_counts['count'] > 1].sort_values(by='count')
    duplicates_sorted['count'] -= 1  # Decrease duplicate counts
    print(f"Number of duplicate links: {len(duplicates_sorted)}")

    # Optimized creation of URL-target pairs and sorting
    url_target_subarrays = df.groupby('url')['target'].apply(list).reset_index().values
    url_target_subarrays_df = pd.DataFrame(url_target_subarrays, columns=['url', 'targets'])
    url_target_subarrays_df['target_count'] = url_target_subarrays_df['targets'].apply(len)
    url_target_subarrays_df = url_target_subarrays_df.sort_values(by='target_count', ascending=False)

    # Convert back to list of lists
    url_target_subarrays_sorted = url_target_subarrays_df[['url', 'targets']].values.tolist()
    print(f"Total subarrays (1:many relationships): {len(url_target_subarrays_sorted)}")

    # Step 1: Split into chunks for processing
    num_chunks = 250
    chunks = create_balanced_chunks(url_target_subarrays, num_chunks)
    print(f"Total chunks: {len(chunks)}")

    # Open the CSV file to record iteration durations
    with open('iteration_durations.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Iteration', 'Duration (Seconds)'])  # CSV header

        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            # Create unique URL index
            retry_operation(create_url_index, driver)

            # Use ThreadPoolExecutor to process node creation in parallel
            with ThreadPoolExecutor(max_workers=12) as executor:
                futures = []
                for i, chunk in enumerate(chunks):
                    nodes_to_create = []

                    # Collect URLs and Targets in bulk
                    for url, targets in chunk:
                        # Add URL to the list
                        nodes_to_create.append({"type": "url", "value": url})
                        # Add all Targets to the list
                        for target in targets:
                            nodes_to_create.append({"type": "target", "value": target})

                    # Track and submit node creation task to thread pool
                    futures.append(executor.submit(track_duration, retry_operation, create_nodes_in_batch, driver, nodes_to_create))

                    # Immediately submit the relationship creation task for the same chunk
                    futures.append(executor.submit(track_duration, retry_operation, create_relationships, driver, chunk))

                # Wait for all futures to complete and handle exceptions
                for future in as_completed(futures):
                    try:
                        duration = future.result()  # Get the duration for the iteration
                        writer.writerow([f"Iteration {futures.index(future) + 1}", duration])  # Log duration to CSV
                    except Exception as e:
                        print(f"Error during multithreaded execution: {e}")

            # Step 3: Create duplicate relationships after processing all chunks
            try:
                with driver.session() as session:
                    query = """
                    UNWIND $duplicates AS duplicate
                    MATCH (u:Page {url: duplicate.url})
                    MATCH (t:Page {url: duplicate.target})
                    WITH u, t, duplicate.count AS duplicate_count
                    UNWIND range(1, duplicate_count) AS n 
                    CREATE (u)-[:LINKS {weight: 1}]->(t)
                    """
                    duplicates_data = duplicates_sorted.to_dict('records')
                    session.execute_write(lambda tx: tx.run(query, duplicates=duplicates_data))

                print(f"Duplicate relationships created successfully.")
            except Exception as e:
                print(f"Error creating duplicate relationships: {e}")

            # Measure total execution time
            end_time = time.perf_counter()
            total_duration = end_time - start_time
            print(f"\nTotal loop time: {total_duration:.4f} seconds.")

if __name__ == "__main__":
    main()
