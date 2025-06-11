import pandas as pd
import os
import json

# Use the absolute path for the combined CSV
combined_csv = "/Users/sugamnema/Desktop/Python/PythonProject2/youtube/data/raw/NEET_Complete_Comments_Database_20250609_152731.csv"
combined_json = "/Users/sugamnema/Desktop/Python/PythonProject2/youtube/data/raw/NEET_Combined_Comments.json"

def main():
    df = pd.read_csv(combined_csv)
    # Convert DataFrame to list of dicts
    comments = df.to_dict(orient="records")
    with open(combined_json, "w", encoding="utf-8") as f:
        json.dump(comments, f, ensure_ascii=False, indent=2)
    print(f"Combined JSON saved: {combined_json}")

if __name__ == "__main__":
    main()
