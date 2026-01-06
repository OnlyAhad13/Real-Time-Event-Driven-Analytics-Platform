from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaConsumer
import asyncio
import json
import logging
import os

router = APIRouter()
logger = logging.getLogger("api")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = "events.processed.v1"

@router.websocket("/events")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket connection established.")
    
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="dashboard-live-viewer",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    
    try:
        await consumer.start()
        logger.info(f"Connected to Kafka topic: {KAFKA_TOPIC}")
        
        async for msg in consumer:
            # Send the event to the frontend
            await websocket.send_json(msg.value)
            
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected.")
    except Exception as e:
        logger.error(f"Error in WebSocket loop: {e}")
    finally:
        await consumer.stop()
        logger.info("Kafka consumer stopped.")
