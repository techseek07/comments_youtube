import re
import unicodedata
from typing import List, Dict
from settings import ALL_KEYWORDS  # ← REMOVED unused TARGET_KEYWORDS import

class TextCleaner:
    def __init__(self):
        self.emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F1E0-\U0001F1FF"  # flags (iOS)
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "]+", flags=re.UNICODE
        )

        # FIXED: Simplified URL pattern, removed unnecessary non-capturing groups and fixed hex pattern
        self.url_pattern = re.compile(r'https?://[a-zA-Z0-9$\-_@.&+!*\\(),/%]+')
        self.mention_pattern = re.compile(r'@\w+')
        self.hashtag_pattern = re.compile(r'#\w+')

    def clean_text(self, text: str, remove_emojis: bool = False, remove_urls: bool = True,
                   remove_mentions: bool = False, remove_hashtags: bool = False) -> str:
        """Clean text with various options."""
        if not text:
            return ""

        # Handle encoding issues
        text = self._fix_encoding(text)

        # Remove URLs
        if remove_urls:
            text = self.url_pattern.sub('', text)

        # Remove mentions
        if remove_mentions:
            text = self.mention_pattern.sub('', text)

        # Remove hashtags
        if remove_hashtags:
            text = self.hashtag_pattern.sub('', text)

        # Remove emojis
        if remove_emojis:
            text = self.emoji_pattern.sub('', text)

        # Remove extra whitespace and special characters
        text = re.sub(r'\s+', ' ', text)  # Multiple spaces to single space
        text = re.sub(r'\n+', ' ', text)  # Multiple newlines to space
        text = text.strip()

        return text

    def _fix_encoding(self, text: str) -> str:
        """Fix common encoding issues."""
        try:
            # Handle BOM and null characters
            text = text.replace('\ufeff', '').replace('\u0000', '')

            # Normalize unicode
            text = unicodedata.normalize('NFKD', text)

            # Ensure proper UTF-8 encoding
            if isinstance(text, bytes):
                text = text.decode('utf-8', errors='ignore')

            return text
        except Exception:
            return str(text)

    def detect_target_keywords(self, text: str) -> Dict[str, List[str]]:
        """Detect target keywords and their variations in text."""
        text_lower = text.lower()
        detected_keywords = {}

        for main_keyword, variations in ALL_KEYWORDS.items():
            found_variations = []
            for variation in variations:
                if variation.lower() in text_lower:
                    found_variations.append(variation)

            if found_variations:
                detected_keywords[main_keyword] = found_variations

        return detected_keywords

    def extract_keywords(self, text: str, min_length: int = 3) -> List[str]:
        """Extract general keywords from text."""
        words = re.findall(r'\b\w+\b', text.lower())
        keywords = [word for word in words if len(word) >= min_length]
        return keywords

    def categorize_comment_sentiment(self, detected_keywords: Dict[str, List[str]]) -> str:
        """Categorize comment sentiment based on detected keywords."""
        if any(keyword in detected_keywords for keyword in ['loved_it', 'great']):
            return 'positive'
        elif 'boring' in detected_keywords:
            return 'negative'
        elif any(keyword in detected_keywords for keyword in ['explanation', 'explain']):
            return 'educational'
        elif any(keyword in detected_keywords for keyword in ['physics_wallah', 'neetprep', 'neet']):
            return 'brand_mention'
        else:
            return 'neutral'

    def extract_emojis(self, text: str) -> List[str]:
        """Extract emojis from text."""
        return self.emoji_pattern.findall(text)
