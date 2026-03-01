from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

app = FastAPI(title="Pagila Rental API - Persona A")

class RentalData(BaseModel):
    customer_id: int
    inventory_id: int
    staff_id: int

DATABASE_URL = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5434/pagila"

engine = create_engine(DATABASE_URL, isolation_level="READ COMMITTED")

@app.post("/rentals")
def create_rental(rental: RentalData):
    try:
        with engine.connect() as conn:
            with conn.begin(): 
                
                lock_query = text("SELECT inventory_id FROM inventory WHERE inventory_id = :inv_id FOR UPDATE;")
                result = conn.execute(lock_query, {"inv_id": rental.inventory_id}).fetchone()
                
                if not result:
                    raise HTTPException(status_code=404, detail="El inventory_id no existe en el catálogo.")

                check_query = text("""
                    SELECT rental_id FROM rental 
                    WHERE inventory_id = :inv_id AND return_date IS NULL;
                """)
                renta_activa = conn.execute(check_query, {"inv_id": rental.inventory_id}).fetchone()
                
                if renta_activa:
                    raise HTTPException(status_code=409, detail="Conflicto: La película ya está rentada.")

                insert_query = text("""
                    INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id)
                    VALUES (NOW(), :inv_id, :cust_id, :staff_id) RETURNING rental_id;
                """)
                
                nuevo_rental_id = conn.execute(insert_query, {
                    "inv_id": rental.inventory_id,
                    "cust_id": rental.customer_id,
                    "staff_id": rental.staff_id
                }).scalar()
                
                return {"mensaje": "Renta creada exitosamente", "rental_id": nuevo_rental_id}

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

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