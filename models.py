from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, Text
from sqlalchemy.orm import relationship
from database import Base

class UserDB(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    password = Column(String)

class WalletDB(Base):
    __tablename__ = "wallets"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    name = Column(String, index=True)
    description = Column(String, nullable=True)

class TransactionDB(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True, index=True)
    wallet_id = Column(Integer, ForeignKey("wallets.id"), nullable=False)
    ticker = Column(String, index=True)
    date = Column(Date)
    quantity = Column(Float)
    price = Column(Float)
    type = Column(String)

class LogoDB(Base):
    __tablename__ = "logos"
    ticker = Column(String, primary_key=True, index=True)
    image_base64 = Column(Text)

class WatchlistDB(Base):
    __tablename__ = "watchlists"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    name = Column(String)
    items = relationship("WatchlistItemDB", back_populates="watchlist", cascade="all, delete-orphan")

class WatchlistItemDB(Base):
    __tablename__ = "watchlist_items"
    id = Column(Integer, primary_key=True, index=True)
    watchlist_id = Column(Integer, ForeignKey("watchlists.id"), nullable=False)
    ticker = Column(String)
    watchlist = relationship("WatchlistDB", back_populates="items")

# NOVA TABELA PARA DADOS DO SISTEMA (Substitui os dados fixos no código)
class SystemConfigDB(Base):
    __tablename__ = "system_configs"
    key_name = Column(String, primary_key=True, index=True)
    config_data = Column(Text) # JSON guardado como texto