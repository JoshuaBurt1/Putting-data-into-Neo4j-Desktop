import pandas as pd
import time
import csv
from neo4j import GraphDatabase

# Neo4j connection parameters
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "randompw001")

# To track processed sub-arrays and keep track of errors
processed_subarrays = set()

def main():
    df = pd.read_csv('sample.csv', sep='\t', on_bad_lines='skip')  # Use '\t' as the separator 
    
    # Node creation subsets:
    unique_targets = df['target'].unique().tolist()
    unique_urls = df['url'].unique().tolist()
    all_urls = list(set(unique_urls) | set(unique_targets))
    print(f"Number of unique links: {len(all_urls)}")

    # URL-target pairs with multiple links to each other
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
    num_chunks = 200
    chunk_size = len(url_target_subarrays) // num_chunks
    chunks = [url_target_subarrays[i:i + chunk_size] for i in range(0, len(url_target_subarrays), chunk_size)]
    total_chunks = len(chunks)
    if total_chunks < num_chunks:
        chunks.extend([[] for _ in range(num_chunks - total_chunks)])  # Pad with empty chunks if necessary

    print(f"Total chunks: {total_chunks}")

    # Function to create the index for 'url' property on 'Page' nodes
    def create_url_index(driver):
        with driver.session() as session:
            try:
                # Attempt to create the index
                session.run("CREATE INDEX FOR (p:Page) ON (p.url)")
                print("Index created on :Page(url)")
            except Exception as e:
                # If the index already exists, Neo4j will throw an error; catch it and handle it
                if "already exists" in str(e):
                    print("Index on :Page(url) already exists, skipping creation.")
                else:
                    # If it's some other error, print the error message
                    print(f"Error creating index: {e}")
                    
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        start_time = time.perf_counter()

        create_url_index(driver)

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

                    print(f"Phase A: {num_urls} URLs and {num_targets} Targets created in chunk {i + 1}/{total_chunks}.")

                except Exception as e:
                    print(f"Error creating nodes for chunk {i + 1}/{total_chunks}: {e}")

                # Phase B: Create relationships for the chunk (no multi-threading)
                try:
                    # Query to MERGE pages and their links
                    with driver.session() as session:
                        query = """
                        UNWIND $pairs AS pair
                        MERGE (a:Page {url: pair[0]})
                        MERGE (b:Page {url: pair[1]})
                        MERGE (a)-[l:LINKS {weight: 1}]->(b)
                        """
                        # Prepare pairs of (URL, Target) for the relationships
                        pairs = [[url, target] for url, targets in chunk for target in targets]
                        session.execute_write(lambda tx: tx.run(query, pairs=pairs))

                    print(f"Phase B: Relationships for chunk {i + 1}/{total_chunks} created.")

                except Exception as e:
                    print(f"Error creating relationships for chunk {i + 1}/{total_chunks}: {e}")

                # End time for this iteration
                iteration_end_time = time.perf_counter()
                iteration_duration = iteration_end_time - iteration_start_time  # Calculate the duration

                # Write the iteration number and duration to the CSV file
                writer.writerow([i + 1, iteration_duration])  # +1 for human-readable iteration numbering
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
                    # Prepare the duplicates data as a list of dictionaries to pass to the query
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
