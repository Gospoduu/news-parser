from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID


metadata = MetaData(schema='public')


sources = Table(
    "sources",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("name", Text, nullable=False),
    Column("rss_url", Text, nullable=False),
    Column("site_url", Text),
    Column(
        "enabled",
        Boolean,
        nullable=False,
        server_default=text("true"),
    ),
    Column(
        "poll_interval_seconds",
        Integer,
        nullable=False,
        server_default=text("300"),
    ),
    UniqueConstraint("rss_url", name="uq_sources_rss_url"),
    CheckConstraint(
        "poll_interval_seconds > 0",
        name="ck_sources_poll_interval_positive",
    ),
)


news = Table(
    "news",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column(
        "source_id",
        BigInteger,
        ForeignKey("sources.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("external_id", Text, nullable=False),
    Column("title", Text, nullable=False),
    Column("link", Text, nullable=False),
    Column("content_text", Text),
    Column(
        "authors",
        ARRAY(Text),
        nullable=False,
        server_default=text("'{}'::text[]"),
    ),
    Column("published_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True)),
    Column("summary", Text),
    Column("section", Text),
    Column(
        "tags",
        ARRAY(Text),
        nullable=False,
        server_default=text("'{}'::text[]"),
    ),
    Column("language", Text),
    Column("image_url", Text),
    Column("image_caption", Text),
    Column(
        "images",
        ARRAY(Text),
        nullable=False,
        server_default=text("'{}'::text[]"),
    ),
    Column(
        "is_active",
        Boolean,
        nullable=True,
        server_default=text("true"),
    ),
    UniqueConstraint(
        "source_id",
        "external_id",
        name="uq_news_source_external_id",
    )
)

Index("ix_news_published_at", news.c.published_at)

outbox = Table(
    "outbox", metadata,
    Column("event_id", UUID(as_uuid=True), primary_key=True),
    Column("article_id", BigInteger, nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    Column("available_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    Column("attempts", Integer, nullable=False, server_default=text("0")),
    Column("last_error", Text),
    Column("published_at", DateTime(timezone=True)),
)
Index("ix_outbox_pending", outbox.c.available_at, outbox.c.created_at,
      postgresql_where=outbox.c.published_at.is_(None))
