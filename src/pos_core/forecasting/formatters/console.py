"""Console output formatting utilities."""

from __future__ import annotations

import re


def sanitize_for_console(text: str) -> str:
    """Sanitize text for console output by removing emojis and HTML tags.
    
    This prevents UnicodeEncodeError on Windows console which uses cp1252 encoding.
    
    Args:
        text: Text that may contain emojis and HTML tags
        
    Returns:
        Sanitized text safe for console output
    """
    # Remove emojis (Unicode characters outside ASCII range)
    # This regex matches emojis and other non-ASCII characters
    text = re.sub(r'[^\x00-\x7F]+', '', text)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    return text

