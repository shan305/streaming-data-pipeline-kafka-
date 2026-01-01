"""
SQLAlchemy models for cold storage (historical trades).
Append-only, replay-safe.
"""
from sqlalchemy import (
    Column,
    Integer,
    String,
    BigInteger,
    Numeric,
    Index,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from config import POSTGRES_URL

Base = declarative_base()


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True)
    symbol = Column(String(20), nullable=False)
    price = Column(Numeric(18, 8), nullable=False)
    quantity = Column(Numeric(18, 8), nullable=False)
    event_time_ms = Column(BigInteger, nullable=False)

    __table_args__ = (
        Index("uq_trade_symbol_time", "symbol", "event_time_ms", unique=True),
    )


engine = create_engine(POSTGRES_URL, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def init_db():
    Base.metadata.create_all(bind=engine)
