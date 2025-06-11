import pandas as pd
import numpy as np
import os

input_csv = "/Users/sugamnema/Desktop/Python/PythonProject2/youtube/data/raw/NEET_Complete_Comments_Database_20250609_155450.csv"
output_dir = "/Users/sugamnema/Desktop/Python/PythonProject2/youtube/data/raw/chunks"
os.makedirs(output_dir, exist_ok=True)

df = pd.read_csv(input_csv)
sample = df.sample(min(100, len(df)))
row_char_counts = sample.apply(lambda row: len(" ".join([str(x) for x in row.values])), axis=1)
avg_chars_per_row = row_char_counts.mean()
tokens_per_row = avg_chars_per_row / 4
max_rows_per_chunk = int(1_000_000 / tokens_per_row)

print(f"Average chars/row: {avg_chars_per_row:.1f}")
print(f"Estimated tokens/row: {tokens_per_row:.1f}")
print(f"Max rows per chunk: {max_rows_per_chunk}")

# Force 15 chunks for extra safety
num_chunks = 15
for i, chunk in enumerate(np.array_split(df, num_chunks)):
    chunk_df = pd.DataFrame(chunk)  # Convert ndarray back to DataFrame
    chunk_path = os.path.join(output_dir, f"comments_chunk_{i+1}.csv")
    chunk_df.to_csv(chunk_path, index=False)
    print(f"Chunk {i+1} saved: {chunk_path} ({len(chunk_df)} rows)")