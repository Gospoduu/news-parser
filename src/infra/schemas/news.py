from calendar import timegm
from datetime import datetime, timezone
from html.parser import HTMLParser
from time import struct_time

from feedparser import FeedParserDict
from pydantic import BaseModel, Field


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.ignored_depth = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in {"script", "style"}:
            self.ignored_depth += 1
        elif tag in {"p", "div", "br", "li", "h1", "h2", "h3"}:
            self.parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style"}:
            self.ignored_depth = max(0, self.ignored_depth - 1)
        elif tag in {"p", "div", "li", "h1", "h2", "h3"}:
            self.parts.append(" ")

    def handle_data(self, data: str) -> None:
        if not self.ignored_depth:
            self.parts.append(data)


def _clean_text(value: str | None, content_type: str = "text/html") -> str | None:
    if not value:
        return None
    if content_type == "text/plain":
        return value.strip() or None
    parser = _TextExtractor()
    parser.feed(value)
    parser.close()
    return " ".join("".join(parser.parts).split()) or None


def _parse_date(value: struct_time | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(timegm(value), tz=timezone.utc)


def _image_url(entry: FeedParserDict) -> tuple[str | None, list[str]]:
    images: list[str] = []
    seen: set[str] = set()

    for field in ("media_content", "media_thumbnail", "enclosures"):
        for media in entry.get(field, []):
            url = media.get("url") or media.get("href")
            is_image = (
                field == "media_thumbnail"
                or (media.get("type") or "").startswith("image/")
                or media.get("medium") == "image"
            )

            if not url or not is_image or url in seen:
                continue

            seen.add(url)
            images.append(url)

    if not images:
        return None, []

    return images[0], images[1:]


class News(BaseModel):
    title: str
    link: str
    external_id: str
    content_text: str | None = None
    authors: list[str] = Field(default_factory=list)
    published_at: datetime | None = None
    updated_at: datetime | None = None
    summary: str | None = None
    section: str | None = None
    tags: list[str] = Field(default_factory=list)
    language: str | None = None
    image_url: str | None = None
    image_caption: str | None = None
    images: list[str] = Field(default_factory=list)

    @staticmethod
    def from_entry(
        entry: FeedParserDict,
        *,
        title: str | None = None,
        url: str | None = None,
        language: str | None = None,
    ) -> "News":
        """Convert a feedparser entry; keyword arguments supply missing values."""
        news_title = entry.get("title") or title
        news_link = entry.get("link") or url
        external_id = entry.get("id") or news_link
        if not news_title or not news_link:
            raise ValueError("News entry must have a title and link")

        authors = [
            author["name"]
            for author in entry.get("authors", [])
            if author.get("name")
        ]
        if not authors and entry.get("author"):
            authors = [entry["author"]]

        tags = [tag["term"] for tag in entry.get("tags", []) if tag.get("term")]
        content = next(
            (
                item
                for item in entry.get("content", [])
                if item.get("value")
                and item.get("type", "text/html")
                in {"text/plain", "text/html", "application/xhtml+xml"}
            ),
            None,
        )
        summary_type = entry.get("summary_detail", {}).get("type", "text/html")
        image_url, images = _image_url(entry)
        return News(
            title=news_title,
            link=news_link,
            external_id=external_id,
            content_text=(
                _clean_text(content["value"], content.get("type", "text/html"))
                if content else None
            ),
            authors=list(dict.fromkeys(authors)),
            published_at=_parse_date(entry.get("published_parsed")),
            updated_at=_parse_date(entry.get("updated_parsed")),
            summary=_clean_text(entry.get("summary"), summary_type),
            tags=list(dict.fromkeys(tags)),
            language=(
                entry.get("language")
                or (content.get("language") if content else None)
                or language
            ),
            image_url=image_url,
            images=images,
        )


class NewNewsEvents(BaseModel):
    news: list[News]