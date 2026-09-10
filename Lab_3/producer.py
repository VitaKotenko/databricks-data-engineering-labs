import asyncio
import json
import requests
import os

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

from dotenv import load_dotenv


load_dotenv("eventhub.env")

event_hub_connection_str = os.getenv("EVENT_HUB_CONNECTION_STR")
event_hub_name = os.getenv("EVENT_HUB_NAME")

tristar_url = "https://ckan2.multimediagdansk.pl/gpsPositions?v=2"


async def main():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=event_hub_connection_str,
        eventhub_name=event_hub_name
    )

    response = requests.get(tristar_url, timeout=20)
    response.raise_for_status()

    data = response.json()

    vehicles = data["vehicles"]
    last_update = data["lastUpdate"]

    async with producer:
        event_data_batch = await producer.create_batch()

        for vehicle in vehicles:
            record = vehicle.copy()
            record["lastUpdate"] = last_update

            event = EventData(
                json.dumps(record, ensure_ascii=False)
            )

            try:
                event_data_batch.add(event)
            except ValueError:
                await producer.send_batch(event_data_batch)

                event_data_batch = await producer.create_batch()
                event_data_batch.add(event)

        if len(event_data_batch) > 0:
            await producer.send_batch(event_data_batch)

    print(f"Sent {len(vehicles)} vehicle events")


if __name__ == "__main__":
    asyncio.run(main())