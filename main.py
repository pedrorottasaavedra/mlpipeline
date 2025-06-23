from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "postgresql://fastapi_user:12345@localhost/predicciones"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class Prediccion(Base):
    __tablename__ = "predicciones"
    id = Column(Integer, primary_key=True, index=True)
    tag = Column(String)
    valor = Column(Float)
    timestamp = Column(String)

Base.metadata.create_all(bind=engine)

app = FastAPI()

class Entrada(BaseModel):
    tag: str
    valor: float
    timestamp: str

@app.post("/insertar")
def insertar_prediccion(entrada: Entrada):
    db = SessionLocal()
    nueva = Prediccion(**entrada.dict())
    db.add(nueva)
    db.commit()
    db.refresh(nueva)
    db.close()
    return {"msg": "Guardado", "id": nueva.id}

@app.get("/predicciones")
def leer_predicciones():
    db = SessionLocal()
    datos = db.query(Prediccion).all()
    db.close()
    return datos
