import json

import aiofiles


async def load_json(file_path: str):
    async with aiofiles.open(file_path, "r") as f:
        try:
            data = await f.read()
            return json.loads(data)
        except json.JSONDecodeError:
            return None


async def save_json(file_path: str, data):
    async with aiofiles.open(file_path, "w") as f:
        await f.write(json.dumps(data, indent=4))
