"""Task 3: bounded lexical lookahead, independent of provider chunk boundaries.

Hold an unfinished word, or a numeric run containing spaces/hyphens. This prevents
releasing an email local part or the beginning of a card number too early.
Oversized candidates fail closed: emit one marker and discard to a boundary.
"""

import re
import string

MARKER = "[REDACTED]"
TOKEN_CHARS = frozenset(string.ascii_letters + string.digits + ".!#$%&'*+-/=?^_`{|}~@")
NUMBER_CHARS = frozenset(string.digits + " -")
EMAIL = r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+"
SSN = r"(?<![0-9])[0-9]{3}-[0-9]{2}-[0-9]{4}(?![0-9])"
CARD = r"(?<![0-9])(?:[0-9][ -]?){12,18}[0-9](?![0-9])"
SENSITIVE = re.compile(f"(?:{EMAIL})|(?:{SSN})|(?:{CARD})")


class StreamingRedactor:
    def __init__(self, max_candidate: int = 320):
        self.max_candidate = max_candidate
        self.buffer = ""
        self.discard: str | None = None

    def _numeric(self) -> bool:
        return bool(self.buffer) and all(c in NUMBER_CHARS for c in self.buffer)

    def _flush(self) -> str:
        result = SENSITIVE.sub(MARKER, self.buffer)
        self.buffer = ""
        return result

    def feed(self, text: str) -> str:
        output: list[str] = []
        for char in text:
            if self.discard:
                if char in TOKEN_CHARS or (self.discard == "number" and char == " "):
                    continue
                self.discard = None
            if char in TOKEN_CHARS:
                # A numeric run can retain spaces for card detection. When a word
                # starts, flush complete numbers but keep the current word prefix.
                if char not in NUMBER_CHARS and self._numeric() and " " in self.buffer:
                    if SENSITIVE.search(self.buffer):
                        output.append(self._flush())
                    else:
                        prefix, suffix = self.buffer.rsplit(" ", 1)
                        self.buffer = prefix
                        output.append(self._flush() + " ")
                        self.buffer = suffix
                self.buffer += char
            elif char == " " and self._numeric():
                self.buffer += char
            else:
                output.append(self._flush())
                output.append(char)
            if len(self.buffer) > self.max_candidate:
                self.discard = "number" if self._numeric() else "word"
                self.buffer = ""
                output.append(MARKER)
        return "".join(output)

    def finish(self) -> str:
        self.discard = None
        return self._flush()
