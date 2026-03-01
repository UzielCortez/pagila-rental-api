from typing import Optional
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime   
from . import models
from .db import get_db          

app = FastAPI(title="Pagila Rental API - Persona A")

class RentalData(BaseModel):
    customer_id: int
    inventory_id: int
    staff_id: int

class PaymentData(BaseModel):
    customer_id: int
    staff_id: int
    amount: float
    rental_id: Optional[int] = None

DATABASE_URL = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5434/pagila"

engine = create_engine(DATABASE_URL, isolation_level="REPEATABLE READ")

@app.post("/rentals")
def create_rental(rental: RentalData, db: Session = Depends(get_db)):
    try:
     
        inventory_item = db.query(models.Inventory)\
            .filter(models.Inventory.inventory_id == rental.inventory_id)\
            .with_for_update()\
            .first()

        if not inventory_item:
            raise HTTPException(status_code=404, detail="Inventory item not found")

        active_rental = db.query(models.Rental)\
            .filter(
                models.Rental.inventory_id == rental.inventory_id,
                models.Rental.return_date == None
            )\
            .first()

        if active_rental:
            raise HTTPException(status_code=400, detail="Item is already rented")


        db_rental = models.Rental(
            inventory_id=rental.inventory_id,
            customer_id=rental.customer_id,
            staff_id=rental.staff_id,
            rental_date=datetime.now()
        )
        
        db.add(db_rental)
        db.commit() 
        db.refresh(db_rental)
        return db_rental

    except Exception as e:
        db.rollback() 
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/returns/{rental_id}")
def return_rental(rental_id: int):
    try:
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="REPEATABLE READ")
            
            with conn.begin():
                check_query = text("""
                    SELECT rental_id, return_date 
                    FROM rental 
                    WHERE rental_id = :rent_id FOR UPDATE;
                """)
                renta = conn.execute(check_query, {"rent_id": rental_id}).fetchone()

                if not renta:
                    raise HTTPException(status_code=404, detail=f"No se encontró la renta con ID {rental_id}.")

                if renta.return_date is not None:
                    return {
                        "mensaje": "Operación exitosa: La película ya había sido devuelta anteriormente.",
                        "rental_id": rental_id,
                        "estado": "Idempotente"
                    }

                update_query = text("""
                    UPDATE rental 
                    SET return_date = NOW() 
                    WHERE rental_id = :rent_id;
                """)
                conn.execute(update_query, {"rent_id": rental_id})

                return {"mensaje": "Devolución registrada exitosamente.", "rental_id": rental_id}

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

@app.post("/payments")
def create_payment(payment: PaymentData):
    try:
        with engine.connect() as conn:
            with conn.begin():
                if payment.rental_id is not None:
                    check_query = text("""
                        SELECT customer_id FROM rental 
                        WHERE rental_id = :rent_id;
                    """)
                    renta = conn.execute(check_query, {"rent_id": payment.rental_id}).fetchone()

                    if not renta:
                        raise HTTPException(status_code=404, detail=f"La renta {payment.rental_id} no existe.")
                    
                    if renta.customer_id != payment.customer_id:
                        raise HTTPException(
                            status_code=403, 
                            detail="Prohibido: Esta renta pertenece a otro cliente. No puedes pagarla."
                        )

                insert_query = text("""
                    INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date)
                    VALUES (:cust_id, :staff_id, :rent_id, :amount, NOW()) RETURNING payment_id;
                """)
                
                nuevo_payment_id = conn.execute(insert_query, {
                    "cust_id": payment.customer_id,
                    "staff_id": payment.staff_id,
                    "rent_id": payment.rental_id,
                    "amount": payment.amount
                }).scalar()

                return {"mensaje": "Pago registrado exitosamente", "payment_id": nuevo_payment_id}

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")