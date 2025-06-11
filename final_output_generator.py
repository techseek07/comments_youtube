# ==============================================================================
# YouTube Comment Prioritization Analyzer (v3.5 - Final Stable Model)
# ==============================================================================
# This version uses the 'gemini-1.5-pro-latest' model, which is stable and
# compatible with the free tier, to fix the 429 billing error.
# ==============================================================================

import os
import pandas as pd
import json
import asyncio
import aiohttp
import time
import re
import logging

# --- Configuration ---
CHUNKS_DIR = '/Users/sugamnema/Desktop/Python/PythonProject2/youtube/data/raw/chunks'
OUTPUT_FILE = '/Users/sugamnema/Desktop/Python/PythonProject2/youtube/data/processed/prioritized_comments_final.csv'
API_KEY_PATH = '/Users/sugamnema/Desktop/Python/PythonProject2/youtube/credentials/google_api_key.txt'
LOG_FILE = 'comment_analysis.log'
CONCURRENT_REQUESTS_BATCH_SIZE = 50

# --- Logging Setup ---
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - CommentID: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def get_api_key(path):
    """Reads the Google Gemini API key from the specified file path."""
    try:
        with open(path, 'r') as f:
            api_key = f.read().strip()
            if not api_key:
                print(f"FATAL ERROR: API Key file at '{path}' is empty.")
                return None
            return api_key
    except FileNotFoundError:
        print(f"FATAL ERROR: API Key file not found at '{path}'.")
        return None
    except Exception as e:
        print(f"FATAL ERROR: Could not read API key file: {e}")
        return None


# --- AI Model Instructions (System Prompt) ---
SYSTEM_PROMPT = """
You are an expert YouTube comment analyst. Your task is to analyze a user comment and return a single, valid JSON object with the specified keys.

**Analysis & Scoring Logic:**
1.  **`priority_score` (Integer 1-10):** Base=5. +2 for confusion. +2 for frustration. +1 for resource request. +3 for urgency. +2 for coaching choice confusion. +2 for admission questions. -4 for sarcasm. -4 for non-actionable praise or like-farming. Cap score 1-10.
2.  **`justification` (String):** Brief reason for the score. E.g., "User is expressing urgent confusion.", "General praise, no action needed."
3.  **`engagement_type` (Array of strings):** From `["help_request", "confusion", "frustration", "resource_request", "general_feedback", "non_actionable"]`.
4.  **`key_phrases` (Array of strings):** 2-5 important keywords.
5.  **`response_strategy` (String):** Based on priority: 8-10 (Urgent support), 5-7 (Direct help), 1-4 (Acknowledge/Ignore).

You MUST return only the JSON object.
"""


def is_comment_filterable(text):
    """Performs initial checks to filter out comments that don't need AI analysis."""
    if not isinstance(text, str): return True
    stripped_text = text.strip()
    if len(stripped_text) < 8: return True
    emoji_pattern = re.compile(
        "[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001FA70-\U0001FAFF\U00002702-\U000027B0]+",
        flags=re.UNICODE)
    if not emoji_pattern.sub('', stripped_text).strip(): return True
    return False


async def analyze_comment_async(session, row_data, semaphore, api_url):
    """Analyzes a single comment and returns a complete dictionary for the final CSV row."""
    comment_text = row_data.get('raw_text', '')
    comment_id = row_data.get('comment_id', 'UNKNOWN')
    video_id = row_data.get('video_id', 'UNKNOWN')
    comment_url = f"https://www.youtube.com/watch?v={video_id}&lc={comment_id}"
    base_info = {'comment_id': comment_id, 'comment_url': comment_url, 'raw_text': comment_text}

    if is_comment_filterable(comment_text):
        return {**base_info, 'priority_score': 1, 'justification': 'Skipped (too short, emoji-only, or empty).',
                'engagement_type': str(['non_actionable']), 'key_phrases': str([]),
                'response_strategy': 'Ignore (spam/non-actionable).'}

    payload = {"contents": [{"parts": [{"text": comment_text}]}],
               "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
               "generationConfig": {"temperature": 0.2, "response_mime_type": "application/json", }}
    headers = {"Content-Type": "application/json"}
    analysis_result = None

    async with semaphore:
        for attempt in range(3):
            try:
                async with session.post(api_url, headers=headers, json=payload, timeout=60) as response:
                    if response.status == 200:
                        response_json = await response.json()
                        analysis_result = json.loads(response_json['candidates'][0]['content']['parts'][0]['text'])
                        break
                    else:
                        error_body = await response.text()
                        logging.error(
                            f"{comment_id} | API Error - Status: {response.status}, Attempt: {attempt + 1}, Body: {error_body[:200]}")
                        await asyncio.sleep(2 * (attempt + 1))
            except Exception as e:
                logging.error(
                    f"{comment_id} | Exception - Type: {type(e).__name__}, Attempt: {attempt + 1}, Details: {str(e)}")
                await asyncio.sleep(2 * (attempt + 1))

    if analysis_result:
        return {**base_info, 'priority_score': analysis_result.get('priority_score', 1),
                'justification': analysis_result.get('justification', 'Analysis failed to generate justification.'),
                'engagement_type': str(analysis_result.get('engagement_type', [])),
                'key_phrases': str(analysis_result.get('key_phrases', [])),
                'response_strategy': analysis_result.get('response_strategy', 'N/A')}
    else:
        return {**base_info, 'priority_score': 3, 'justification': 'API analysis failed after all retries.',
                'engagement_type': str(['']), 'key_phrases': str(['']), 'response_strategy': 'Manual review needed.'}


async def main():
    start_time = time.time()
    api_key = get_api_key(API_KEY_PATH)
    if not api_key: return

    # *** THE FINAL, CORRECT MODEL NAME ***
    # This model is stable and works with the free tier.
    model_name = "gemini-1.5-pro-latest"
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    if not os.path.isdir(CHUNKS_DIR):
        print(f"FATAL ERROR: Chunks directory not found at '{CHUNKS_DIR}'")
        return

    print("Reading and consolidating all CSV chunks...")
    try:
        all_comments_df = pd.concat(
            [pd.read_csv(os.path.join(CHUNKS_DIR, f)) for f in os.listdir(CHUNKS_DIR) if f.endswith('.csv')],
            ignore_index=True)
    except Exception as e:
        print(f"An error occurred while reading CSVs: {e}")
        return

    all_comments_df.dropna(subset=['raw_text'], inplace=True)
    total_comments = len(all_comments_df)
    print(f"Consolidated {total_comments} comments to process.")

    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    print(f"Errors will be logged to: {os.path.abspath(LOG_FILE)}")

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_BATCH_SIZE)
    print(f"Starting analysis with model '{model_name}' and concurrency of {CONCURRENT_REQUESTS_BATCH_SIZE}...")

    sample_output_printed = False

    async with aiohttp.ClientSession() as session:
        tasks = [analyze_comment_async(session, row.to_dict(), semaphore, api_url) for _, row in
                 all_comments_df.iterrows()]
        final_results = []
        processed_count = 0
        for future in asyncio.as_completed(tasks):
            result = await future
            processed_count += 1
            if result:
                final_results.append(result)
                if not sample_output_printed and result.get('priority_score', 0) > 1:
                    print("\n--- First Successful Analysis Sample ---")
                    for key, value in result.items():
                        print(f"  {key}: {value}")
                    print("----------------------------------------\n")
                    sample_output_printed = True

            if processed_count % 100 == 0:
                print(f"  ... Analyzed {processed_count} / {total_comments} comments")

    print("\nAnalysis complete. Assembling and saving final CSV...")
    if not final_results:
        print("No comments were successfully processed.")
        return

    final_df = pd.DataFrame(final_results)
    final_df = final_df.sort_values(by='priority_score', ascending=False)
    final_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')

    end_time = time.time()
    print("-" * 50)
    print(f"SUCCESS: Created prioritized comments file at: {OUTPUT_FILE}")
    print(f"Total comments in final CSV: {len(final_df)} / {total_comments}")
    print(f"Total execution time: {(end_time - start_time) / 60:.2f} minutes.")
    print("-" * 50)


if __name__ == '__main__':
    asyncio.run(main())