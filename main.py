from fastapi import FastAPI, Query
from typing import Optional
from datetime import datetime
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import List
from fastapi.responses import JSONResponse


# Configuración de base de datos
DATABASE_URL = "postgresql://fastapi_user:12345@localhost/predicciones"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# Modelo SQLAlchemy
class Prediccion(Base):
    __tablename__ = "predicciones"
    id = Column(Integer, primary_key=True, index=True)
    tag = Column(String)
    valor = Column(Float)
    timestamp = Column(DateTime)

# Modelo de entrada con Pydantic
class Entrada(BaseModel):
    tag: str
    valor: float
    timestamp: datetime

app = FastAPI()

# Endpoint para insertar una predicción
@app.post("/insertar")
def insertar_predicciones(entradas: List[Entrada]):
    db = SessionLocal()
    nuevas = [Prediccion(**e.dict()) for e in entradas]
    db.add_all(nuevas)
    db.commit()
    for nueva in nuevas:
        db.refresh(nueva)
    db.close()
    return {"msg": "Guardados", "cantidad": len(nuevas)}
# Endpoint para consultar predicciones con filtros
@app.get("/predicciones")
def leer_predicciones(
    tag: Optional[str] = Query(None),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None)
):
    db = SessionLocal()
    query = db.query(Prediccion)

    if tag:
        query = query.filter(Prediccion.tag == tag)
    if start:
        query = query.filter(Prediccion.timestamp >= start)
    if end:
        query = query.filter(Prediccion.timestamp <= end)

    datos = query.all()
    db.close()
    return datos
@app.get("/api/proxy")
def proxy_grafana():
    db = SessionLocal()
    predicciones = db.query(Prediccion).all()
    db.close()
    return JSONResponse(content=[
        {
            "tag": p.tag,
            "valor": p.valor,
            "timestamp": p.timestamp.isoformat()
        } for p in predicciones
    ])

