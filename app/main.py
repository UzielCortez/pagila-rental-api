from typing import Optional
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime   
from . import models
from .db import get_db      
import time  
from sqlalchemy.exc import DBAPIError 

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
    max_retries = 3
    
    # Iniciamos el bucle de intentos (Retry Logic)
    for attempt in range(max_retries):
        try:
            # 1. Bloqueo Pesimista (SELECT ... FOR UPDATE)
            # Buscamos el inventario y bloqueamos la fila para que nadie más la toque
            inventory = db.query(models.Inventory).filter(
                models.Inventory.inventory_id == rental.inventory_id
            ).with_for_update().first()

            # Validación 1: ¿Existe el inventario?
            if not inventory:
                raise HTTPException(status_code=404, detail="Inventory not found")

            # Validación 2: Lógica de Negocio (¿Ya está rentado?)
            # Buscamos si hay una renta activa (return_date es NULL)
            active_rental = db.query(models.Rental).filter(
                models.Rental.inventory_id == rental.inventory_id,
                models.Rental.return_date == None
            ).first()

            if active_rental:
                # Si ya está rentado, lanzamos error 400 y salimos.
                # No necesitamos reintentar aquí porque es una regla de negocio.
                raise HTTPException(status_code=400, detail="Item is already rented")

            # 2. Crear la nueva renta
            new_rental = models.Rental(
                rental_date=datetime.now(),
                inventory_id=rental.inventory_id,
                customer_id=rental.customer_id,
                staff_id=rental.staff_id
            )

            db.add(new_rental)
            
            # 3. Commit (Aquí es donde Postgres podría lanzar el Deadlock)
            db.commit()
            db.refresh(new_rental)
            
            # Si llegamos aquí, todo fue éxito. Retornamos y rompemos el bucle.
            return new_rental

        except DBAPIError as e:
            # Si ocurre un error de base de datos, hacemos rollback primero
            db.rollback()
            
            # Convertimos el error a texto para buscar las palabras clave
            error_msg = str(e).lower()
            
            # Verificamos si es un Deadlock (código 40P01) o Error de Serialización (40001)
            if "deadlock" in error_msg or "serialization" in error_msg:
                # Si nos quedan intentos, esperamos y volvemos a probar
                if attempt < max_retries - 1:
                    time.sleep(0.5)  # Espera de 0.5 segundos (Backoff)
                    print(f"⚠️ Conflicto de concurrencia (Deadlock). Reintentando... ({attempt+1}/{max_retries})")
                    continue 
            
            # Si no fue deadlock o se acabaron los intentos, lanzamos error 500 real
            raise HTTPException(status_code=500, detail="Database concurrency error")
            
        except HTTPException as he:
            # Si fue un error 400 o 404 generado por nosotros, lo dejamos pasar
            db.rollback()
            raise he
            
        except Exception as e:
            # Cualquier otro error inesperado
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