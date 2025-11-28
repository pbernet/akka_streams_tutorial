"""
GLiNER NER Service
FastAPI service for Named Entity Recognition using GLiNER model.
Specifically designed to extract person names from text.
"""

import logging
import time
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from gliner import GLiNER
import uvicorn

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global model instance
model: Optional[GLiNER] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager to load model on startup and cleanup on shutdown.
    """
    global model
    logger.info("Loading GLiNER model...")
    start_time = time.time()
    
    try:
        # Load GLiNER model - using medium version for balance of speed/accuracy
        model = GLiNER.from_pretrained("urchade/gliner_medium-v2.1")
        load_time = time.time() - start_time
        logger.info(f"GLiNER model loaded successfully in {load_time:.2f}s")
    except Exception as e:
        logger.error(f"Failed to load GLiNER model: {e}")
        raise
    
    yield
    
    # Cleanup
    logger.info("Shutting down GLiNER service")
    model = None

# Initialize FastAPI app with lifespan manager
app = FastAPI(
    title="GLiNER NER Service",
    description="Named Entity Recognition service using GLiNER for person extraction",
    version="1.0.0",
    lifespan=lifespan
)

# Request/Response Models
class PersonExtractionRequest(BaseModel):
    """Request model for person extraction endpoint"""
    text: str = Field(..., description="Text content to extract persons from", min_length=1)
    threshold: float = Field(default=0.5, description="Confidence threshold for entity extraction", ge=0.0, le=1.0)

class PersonEntity(BaseModel):
    """Represents a person entity found in text"""
    text: str = Field(..., description="The person name text")
    start: int = Field(..., description="Start position in original text")
    end: int = Field(..., description="End position in original text") 
    score: float = Field(..., description="Confidence score for this entity")

class PersonExtractionResponse(BaseModel):
    """Response model for person extraction"""
    persons: List[PersonEntity] = Field(..., description="List of person entities found")
    processing_time_ms: float = Field(..., description="Processing time in milliseconds")
    text_length: int = Field(..., description="Length of input text")

class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    model_loaded: bool
    service_name: str
    version: str

class ErrorResponse(BaseModel):
    """Error response model"""
    error: str
    detail: Optional[str] = None

# API Endpoints

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy" if model is not None else "unhealthy",
        model_loaded=model is not None,
        service_name="GLiNER NER Service",
        version="1.0.0"
    )

@app.post("/extract-persons", response_model=PersonExtractionResponse)
async def extract_persons(request: PersonExtractionRequest):
    """
    Extract person entities from text using GLiNER model.
    
    This endpoint is specifically designed to work with the WikipediaEditsAnalyser
    Scala application for consistent person name extraction.
    """
    if model is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="GLiNER model not loaded"
        )
    
    start_time = time.time()
    
    try:
        # Extract person entities using GLiNER
        # GLiNER expects labels as a list - we specify "person" as the entity type
        entities = model.predict_entities(
            request.text, 
            labels=["person"], 
            threshold=request.threshold
        )
        
        # Convert GLiNER output to our response format
        person_entities = []
        for entity in entities:
            person_entities.append(PersonEntity(
                text=entity["text"],
                start=entity["start"],
                end=entity["end"],
                score=entity["score"]
            ))
        
        processing_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        
        logger.info(f"Extracted {len(person_entities)} persons from text of length {len(request.text)} in {processing_time:.2f}ms")
        
        return PersonExtractionResponse(
            persons=person_entities,
            processing_time_ms=processing_time,
            text_length=len(request.text)
        )
        
    except Exception as e:
        logger.error(f"Error during person extraction: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Person extraction failed: {str(e)}"
        )

@app.get("/")
async def root():
    """Root endpoint with service information"""
    return {
        "service": "GLiNER NER Service",
        "version": "1.0.0",
        "description": "Named Entity Recognition service for person extraction",
        "endpoints": {
            "health": "/health",
            "extract_persons": "/extract-persons",
            "docs": "/docs"
        }
    }

if __name__ == "__main__":
    # For development only - in production, use docker with uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8085,
        log_level="info",
        reload=False  # Set to True for development
    )
