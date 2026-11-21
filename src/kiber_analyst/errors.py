class IngestError(Exception):
    """Expected, user-facing pipeline error."""


class FetchError(IngestError):
    """Archive fetch or validation failed."""


class ParseError(IngestError):
    """Telegram export could not be normalized safely."""


class LoadError(IngestError):
    """Knowledge database load could not be completed."""


class EmbedError(IngestError):
    """Embedding generation or persistence failed safely."""


class SearchError(IngestError):
    """Knowledge corpus search failed safely."""
