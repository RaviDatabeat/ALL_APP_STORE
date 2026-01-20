import pandas as pd
import asyncio
from asyncio import Semaphore
from ddgs import DDGS
import re
import time
import random
from difflib import SequenceMatcher
from rapidfuzz import fuzz, process

# --- Helper functions ---
def normalize(text: str) -> str:
    return re.sub(r'[^a-z0-9]', '', text.lower())

def clean_search_query(app_name: str) -> str:
    """Remove special characters like hyphens from app name for better search"""
    cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', app_name)
    return re.sub(r'\s+', ' ', cleaned).strip()

def get_search_variations(app_name: str) -> list:
    """Generate multiple search variations for better matching"""
    variations = []
    
    # Original cleaned name
    cleaned = clean_search_query(app_name)
    variations.append(cleaned)
    
    # Split into words and try different combinations
    words = re.findall(r'[a-zA-Z0-9]+', app_name.lower())
    
    if len(words) > 1:
        # Try individual significant words (longer words first)
        significant_words = [word for word in words if len(word) > 3]
        variations.extend(significant_words)
        
        # Try word combinations (2-word phrases)
        for i in range(len(words) - 1):
            variations.append(f"{words[i]} {words[i+1]}")
    
    # Remove duplicates and empty variations
    variations = list(set([v for v in variations if v.strip()]))
    
    print(f"  Search variations for '{app_name}': {variations}")
    return variations

def similarity_sequence(a: str, b: str) -> float:
    """Return similarity ratio using SequenceMatcher (0 to 1)"""
    return SequenceMatcher(None, a, b).ratio()

def similarity_token(a: str, b: str) -> float:
    """Return similarity ratio using rapidfuzz token set ratio (handles acronyms, word order)"""
    return fuzz.token_set_ratio(a, b) / 100.0

def extract_app_name_from_url(url: str) -> str:
    """Extract app name from Roku URL like '.../tunein'"""
    if pd.isna(url):
        return ""
    
    url = str(url).strip()
    if '/' in url:
        app_name = url.rstrip('/').split('/')[-1]
        return app_name
    return url

def _search_ddg(app_name: str):
    """Blocking DDGS search; to be run in executor"""
    time.sleep(random.uniform(0.05, 0.2))  # polite delay
    
    # Get multiple search variations
    search_variations = get_search_variations(app_name)
    all_results = []
    
    with DDGS() as ddgs:
        for variation in search_variations:
            try:
                print(f"  Searching variation: '{variation}'")
                results = [r for r in ddgs.text(f"{variation}.com", max_results=5)]  # Fewer results per variation
                all_results.extend(results)
                time.sleep(random.uniform(0.05, 0.1))  # Small delay between searches
            except Exception as e:
                print(f"  Error searching for '{variation}': {e}")
                continue

    if not all_results:
        return None

    # Remove duplicates by URL
    seen_urls = set()
    unique_results = []
    for r in all_results:
        href = r.get("href", "")
        if href and href not in seen_urls:
            seen_urls.add(href)
            unique_results.append(r)

    print(f"  Total unique results: {len(unique_results)}")
    
    norm_query = normalize(app_name)
    candidate_urls = []

    for r in unique_results:
        href = r.get("href", "")
        if not href or "wikipedia.org" in href or "channelstore.roku.com" in href:
            continue

        domain = re.sub(r"^https?://(www\.)?", "", href).split("/")[0]
        domain_norm = normalize(domain)
        
        # Calculate multiple similarity scores
        seq_similarity = similarity_sequence(norm_query, domain_norm)
        token_similarity = similarity_token(app_name, domain)
        
        # Check for partial word matches (important for cases like "zola-levitt-ministries" vs "levitt")
        words_in_app = re.findall(r'[a-zA-Z0-9]+', app_name.lower())
        words_in_domain = re.findall(r'[a-zA-Z0-9]+', domain.lower())
        
        partial_match_score = 0.0
        matching_words = set(words_in_app) & set(words_in_domain)
        if matching_words:
            # Give higher score for matching significant words
            significant_matches = [word for word in matching_words if len(word) > 3]
            if significant_matches:
                partial_match_score = 0.8  # Boost for significant word matches
        
        # Also check if domain contains acronym/initial match
        words = re.findall(r'[a-zA-Z0-9]+', app_name.lower())
        initials = ''.join([word[0] for word in words if word])
        
        has_acronym_match = False
        if len(initials) >= 2 and initials in domain_norm:
            has_acronym_match = True
            acronym_similarity = 0.7
        else:
            acronym_similarity = 0.0
        
        # Weighted combined score with partial matching
        combined_score = (
            seq_similarity * 0.2 + 
            token_similarity * 0.4 + 
            partial_match_score * 0.3 +
            acronym_similarity * 0.1
        )
        
        print(f"  {app_name} vs {domain}: seq={seq_similarity:.2f}, token={token_similarity:.2f}, partial={partial_match_score:.2f}, combined={combined_score:.2f}")
        
        # More flexible acceptance criteria
        if (seq_similarity >= 0.4 or token_similarity >= 0.5 or partial_match_score >= 0.5 or 
            combined_score >= 0.4 or len(matching_words) > 0):
            candidate_urls.append((combined_score, href, domain, len(matching_words)))

    if candidate_urls:
        # Prioritize results with more matching words, then highest combined score
        best_match = max(candidate_urls, key=lambda x: (x[3], x[0]))  # Word count first, then score
        print(f"  BEST MATCH: {best_match[2]} (score: {best_match[0]:.2f}, matching words: {best_match[3]})")
        return best_match[1]

    # fallback: return first non-Wikipedia result
    non_wiki_results = [r for r in unique_results if "wikipedia.org" not in r.get("href", "")]
    if non_wiki_results:
        fallback_url = non_wiki_results[0].get("href", None)
        print(f"  FALLBACK: Using first non-Wikipedia result: {fallback_url}")
        return fallback_url

    return None

async def get_developer_url(app_name: str, sem: Semaphore):
    """Async wrapper using a thread pool for the blocking DDGS search"""
    async with sem:
        print(f"Searching developer URL for: {app_name}")
        loop = asyncio.get_event_loop()
        url = await loop.run_in_executor(None, _search_ddg, app_name)
        print(f"Found URL for {app_name}: {url}")
        return app_name, url

# --- Main async function ---
async def main():
    input_file = r"store_validators\roku_store\roku_app_data_updated.csv"
    df = pd.read_csv(input_file)

    # First, let's see what columns we actually have
    print("Columns in CSV file:", df.columns.tolist())
    
    # Check if we have a URL column (try common names)
    url_column = None
    for col in ['url', 'URL', 'link', 'website', 'domain']:
        if col in df.columns:
            url_column = col
            break
    
    if not url_column:
        print("Error: No URL column found. Available columns:", df.columns.tolist())
        return
    
    print(f"Using column '{url_column}' for extracting app names")
    
    # Extract app names from the URL column
    print("Extracting app names from URLs...")
    df["appName"] = df[url_column].apply(extract_app_name_from_url)
    
    # Show some samples
    print("Sample extracted app names:")
    for i, (url, app_name) in enumerate(zip(df[url_column].head(5), df["appName"].head(5))):
        print(f"  {url} -> {app_name}")
    
    # Filter out empty app names
    df = df[df["appName"] != ""]
    print(f"After filtering empty names: {len(df)} rows")
    
    # FILTER: Only process apps where appstore_developer_url is empty
    if 'appstore_developer_url' in df.columns:
        existing_urls_count = df['appstore_developer_url'].notna().sum()
        print(f"Apps with existing developer URLs: {existing_urls_count}")
        df_to_process = df[df['appstore_developer_url'].isna()]
        print(f"Apps to process (without developer URLs): {len(df_to_process)}")
    else:
        print("No 'appstore_developer_url' column found, processing all apps")
        df_to_process = df

    # Deduplicate app names from the filtered DataFrame
    unique_app_names = df_to_process["appName"].dropna().unique()
    print(f"Total unique apps to search: {len(unique_app_names)}")

    sem = Semaphore(100)  # Reduced concurrency due to multiple searches per app
    tasks = [get_developer_url(name, sem) for name in unique_app_names]

    results = await asyncio.gather(*tasks)

    # Convert results to DataFrame
    df_devs = pd.DataFrame(results, columns=["appName", "Developer_URL"])

    # Merge back to original DataFrame - update only empty developer URLs
    if 'appstore_developer_url' in df.columns:
        df_result = df.copy()
        for app_name, dev_url in results:
            mask = (df_result['appName'] == app_name) & (df_result['appstore_developer_url'].isna())
            df_result.loc[mask, 'appstore_developer_url'] = dev_url
    else:
        df_result = df.merge(df_devs, on="appName", how="left")
        df_result.rename(columns={'Developer_URL': 'appstore_developer_url'}, inplace=True)

    # Save to CSV
    output_file = r"store_validators\roku_store\roku_store_searching_afinal_apps.csv"
    df_result.to_csv(output_file, index=False)
    print(f"Saved {len(df_result)} apps with developer URLs to: {output_file}")

# --- Run the async main ---
if __name__ == "__main__":
    asyncio.run(main())

