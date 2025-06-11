import pandas as pd
import os
import re
from glob import glob

# Directory containing your chunked CSVs
chunks_dir = "/Users/sugamnema/Desktop/Python/PythonProject2/youtube/data/raw/chunks"
output_csv = "/Users/sugamnema/Desktop/Python/PythonProject2/youtube/data/raw/NEET_Prioritized_Comments_All.csv"

# Define keyword lists for each criterion
resource_keywords = [
    "notes", "provide", "pdf", "material", "resources", "link", "download", "chapter", "saree chapter", "pyq", "question paper", "test series", "solution", "handwritten", "slides", "summary", "please send", "send", "share"
]
confusion_keywords = [
    "confused", "confusing", "not clear", "unclear", "which", "should I", "what to do", "don't know", "lost", "which coaching", "which material", "which teacher", "which faculty", "which channel", "which book"
]
urgency_keywords = [
    "urgent", "asap", "please help", "help me", "need help", "immediately", "very important", "please reply", "please answer", "please respond", "please guide", "please suggest", "please recommend"
]
frustration_keywords = [
    "angry", "frustrated", "disappointed", "hate", "bad", "worst", "not good", "not helpful", "waste", "useless", "not satisfied", "not working", "not worth", "not happy", "not satisfied", "not explained", "not teaching", "not clear", "not proper"
]
admission_keywords = [
    "admission", "where to take admission", "which coaching", "join coaching", "join institute", "join class", "join batch", "join neet", "join preparation", "join exam", "join test series"
]
sarcasm_patterns = [
    r"\bsure\b.*\bnot\b", r"\byeah right\b", r"\bgreat\b.*\bnot\b", r"\bthanks a lot\b.*\bnot\b"
]
praise_patterns = [
    r"\bgreat video\b", r"\bawesome\b", r"\bexcellent\b", r"\bgood job\b", r"\bwell done\b", r"\bnice\b", r"\bhelpful\b", r"\bthank you\b", r"\bthanks\b"
]
spam_patterns = [
    r"http[s]?://", r"subscribe", r"giveaway", r"free", r"visit my channel", r"check out", r"promo code"
]

def is_resource_request(text):
    return any(kw in text.lower() for kw in resource_keywords)

def is_confused(text):
    return any(kw in text.lower() for kw in confusion_keywords)

def is_urgent(text):
    return any(kw in text.lower() for kw in urgency_keywords)

def is_frustrated(text):
    return any(kw in text.lower() for kw in frustration_keywords)

def is_admission_query(text):
    return any(kw in text.lower() for kw in admission_keywords)

def is_sarcastic(text):
    return any(re.search(pat, text.lower()) for pat in sarcasm_patterns)

def is_general_praise(text):
    return any(re.search(pat, text.lower()) for pat in praise_patterns)

def is_spam(text):
    return any(re.search(pat, text.lower()) for pat in spam_patterns)

def get_priority_score(text):
    score = 5
    reasons = []
    text_lower = text.lower()

    # Exclusion: spam/irrelevant
    if is_spam(text):
        return 0, "Spam/irrelevant"

    # Resource request
    if is_resource_request(text):
        score += 1  # Decreased from +2 to +1
        reasons.append("Resource request")

    # Confusion
    if is_confused(text):
        score += 2  # Increased from +1 to +2
        reasons.append("Confused about topic/choice/resource")

    # Urgency/needy
    if is_urgent(text):
        score += 3
        reasons.append("Urgent/needy")

    # Frustration/anger (only if directed at channel/teacher/video)
    if is_frustrated(text) and any(x in text_lower for x in ["channel", "teacher", "faculty", "video"]):
        score += 2  # Increased from +1 to +2
        reasons.append("Frustration/anger at channel/teacher/video")

    # Admission/preparation queries
    if is_admission_query(text):
        score += 2
        reasons.append("Admission/preparation query")

    # Sarcasm
    if is_sarcastic(text):
        score -= 4
        reasons.append("Sarcasm detected")

    # General praise/feedback only
    if is_general_praise(text) and not (is_resource_request(text) or is_confused(text) or is_urgent(text) or is_frustrated(text) or is_admission_query(text)):
        score -= 3
        reasons.append("General praise/feedback only")

    # Clamp score to minimum 0
    score = max(score, 0)
    response_strategy = "; ".join(reasons) if reasons else "General/neutral"
    return score, response_strategy

# Aggregate all chunked CSVs
all_files = sorted(glob(os.path.join(chunks_dir, "comments_chunk_*.csv")))
all_comments = []

for file in all_files:
    df = pd.read_csv(file)
    for idx, row in df.iterrows():
        text = str(row.get("raw_text", ""))
        score, reason = get_priority_score(text)
        row_dict = row.to_dict()
        row_dict["priority_score"] = score
        row_dict["response_strategy"] = reason
        all_comments.append(row_dict)

# Create DataFrame, sort, and save
result_df = pd.DataFrame(all_comments)
result_df = result_df.sort_values("priority_score", ascending=False)
result_df.to_csv(output_csv, index=False)
print(f"Prioritized and sorted comments saved to: {output_csv}")