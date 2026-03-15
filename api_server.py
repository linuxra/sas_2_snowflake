"""FastAPI backend for SAS-to-Snowflake converter."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Dict, List, Optional
import os

from sas_to_snowflake import SASToSnowflakeConverter

app = FastAPI(title="SAS to Snowflake Converter")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ConvertRequest(BaseModel):
    sas_code: str
    macro_vars: Optional[Dict[str, str]] = None


class ConvertResponse(BaseModel):
    sql: str
    warnings: List[str]


@app.post("/api/convert", response_model=ConvertResponse)
def convert(req: ConvertRequest):
    converter = SASToSnowflakeConverter(macro_vars=req.macro_vars)
    try:
        result = converter.convert(req.sas_code)
    except Exception as e:
        return ConvertResponse(sql="", warnings=[f"Error: {e}"])
    return ConvertResponse(sql=result.sql, warnings=result.warnings)


# Serve built frontend in production
dist = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.isdir(dist):
    app.mount("/", StaticFiles(directory=dist, html=True), name="frontend")
