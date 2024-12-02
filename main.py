from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import hurtos, geo_data, camaras, gdo, comisarias, homicidios, medidas_correctivas, percepcion, rentas_criminales

app = FastAPI(
    title="GAIA API",
    description="API para el sistema GAIA de visualización de datos geoespaciales",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(hurtos.router)
# app.include_router(geo_data.router)
# app.include_router(camaras.router)
# app.include_router(gdo.router)
# app.include_router(comisarias.router)
# app.include_router(homicidios.router)
# app.include_router(medidas_correctivas.router)
# app.include_router(percepcion.router)
# app.include_router(rentas_criminales.router)