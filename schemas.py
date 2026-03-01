from pydantic import BaseModel
from typing import Optional, List
from datetime import date

class UserAuth(BaseModel): 
    username: str
    password: str

class WalletCreate(BaseModel): 
    user_id: int
    name: str
    description: Optional[str] = None

class TransactionCreate(BaseModel): 
    wallet_id: int
    ticker: str
    date: date
    quantity: float
    price: float
    type: str

class LogoCreate(BaseModel): 
    ticker: str
    image_base64: str

class WatchlistCreate(BaseModel): 
    user_id: int
    name: str

class WatchlistItemCreate(BaseModel): 
    ticker: str