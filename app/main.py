from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

app = FastAPI(title="Pagila Rental API - Persona A")

# --- MODELOS DE DATOS ---
class RentalData(BaseModel):
    customer_id: int
    inventory_id: int
    staff_id: int

# --- CONFIGURACIÓN DE BASE DE DATOS (SQLAlchemy) ---
# Formato: postgresql+psycopg2://usuario:contraseña@host:puerto/base_de_datos
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@localhost:5434/pagila"

# Creamos el "Engine" (El motor que maneja las conexiones). 
# Aquí cumplimos la restricción del nivel de aislamiento.
engine = create_engine(DATABASE_URL, isolation_level="READ COMMITTED")


# --- ENDPOINTS ---
@app.post("/rentals")
def create_rental(rental: RentalData):
    
    # Abrimos la conexión con el motor
    try:
        with engine.connect() as conn:
            # Iniciamos la transacción (Hace el BEGIN automático)
            with conn.begin(): 
                
                # 1. ESTRATEGIA: LOCK PESIMISTA (text() envuelve nuestro SQL crudo)
                lock_query = text("SELECT inventory_id FROM inventory WHERE inventory_id = :inv_id FOR UPDATE;")
                # Pasamos los parámetros de forma segura con un diccionario
                result = conn.execute(lock_query, {"inv_id": rental.inventory_id}).fetchone()
                
                if not result:
                    raise HTTPException(status_code=404, detail="El inventory_id no existe en el catálogo.")

                # 2. REGLA DE NEGOCIO: Validar que no esté rentada
                check_query = text("""
                    SELECT rental_id FROM rental 
                    WHERE inventory_id = :inv_id AND return_date IS NULL;
                """)
                renta_activa = conn.execute(check_query, {"inv_id": rental.inventory_id}).fetchone()
                
                if renta_activa:
                    # Al lanzar este error, SQLAlchemy hace el ROLLBACK automáticamente por nosotros.
                    raise HTTPException(status_code=409, detail="Conflicto: La película ya está rentada.")

                # 3. INSERCIÓN
                insert_query = text("""
                    INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id)
                    VALUES (NOW(), :inv_id, :cust_id, :staff_id) RETURNING rental_id;
                """)
                
                # .scalar() es un truco de SQLAlchemy para devolvernos directamente el primer valor (el ID nuevo)
                nuevo_rental_id = conn.execute(insert_query, {
                    "inv_id": rental.inventory_id,
                    "cust_id": rental.customer_id,
                    "staff_id": rental.staff_id
                }).scalar()
                
                # Si llegamos aquí sin errores, el bloque 'with conn.begin():' hará el COMMIT automático.
                return {"mensaje": "Renta creada exitosamente", "rental_id": nuevo_rental_id}

    except HTTPException:
        # Dejamos pasar nuestros errores controlados (404, 409)
        raise
    except SQLAlchemyError as e:
        # Atrapamos errores específicos de la base de datos
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {str(e)}")
    except Exception as e:
        # Atrapamos cualquier otro error
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")