from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from .db import Base  # Importamos la Base que definimos en db.py

class Inventory(Base):
    __tablename__ = "inventory"

    inventory_id = Column(Integer, primary_key=True, index=True)
    film_id = Column(Integer)
    store_id = Column(Integer)
    last_update = Column(DateTime, default=datetime.now)

class Rental(Base):
    __tablename__ = "rental"

    rental_id = Column(Integer, primary_key=True, index=True)
    rental_date = Column(DateTime, default=datetime.now)
    inventory_id = Column(Integer, ForeignKey("inventory.inventory_id"))
    customer_id = Column(Integer)
    return_date = Column(DateTime, nullable=True)
    staff_id = Column(Integer)
    last_update = Column(DateTime, default=datetime.now)

class Payment(Base):
    __tablename__ = "payment"

    payment_id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer)
    staff_id = Column(Integer)
    rental_id = Column(Integer, ForeignKey("rental.rental_id"))
    amount = Column(Float)
    payment_date = Column(DateTime, default=datetime.now)