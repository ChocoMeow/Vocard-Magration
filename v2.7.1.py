import abc
import asyncio
import io
import os
import base64
import struct
import sys
import json
import dataclasses

from motor.motor_asyncio import AsyncIOMotorClient
from typing import Union, BinaryIO, Optional, Dict, Mapping, Callable, Any, Final

# Decode
@dataclasses.dataclass(frozen=True)
class Codec:
    encoding: str
    error_handler: str

    def encode(self, data: str) -> bytes:
        return data.encode(self.encoding, self.error_handler)

    def decode(self, data: bytes) -> str:
        return data.decode(self.encoding, self.error_handler)

UTF8 = Codec("utf-8", "surrogatepass")

_FORMAT_BOOL = "?"
_FORMAT_BYTE = "b"
_FORMAT_INT = ">i"
_FORMAT_LONG = ">q"
_FORMAT_USHORT = ">H"

class HasStream(abc.ABC):
    @property
    @abc.abstractmethod
    def stream(self) -> BinaryIO:
        ...

class Reader(HasStream):
    def __init__(self, stream: Union[BinaryIO, HasStream]) -> None:
        self._stream: BinaryIO = stream.stream if isinstance(stream, HasStream) else stream

    @property
    def stream(self) -> BinaryIO:
        return self._stream

    def read_bool(self) -> bool:
        return struct.unpack(_FORMAT_BOOL, self._stream.read(1))[0]

    def read_byte(self) -> int:
        return struct.unpack(_FORMAT_BYTE, self._stream.read(1))[0]

    def read_int(self) -> int:
        return struct.unpack(_FORMAT_INT, self._stream.read(4))[0]

    def read_long(self) -> int:
        return struct.unpack(_FORMAT_LONG, self._stream.read(8))[0]

    def read_ushort(self) -> int:
        return struct.unpack(_FORMAT_USHORT, self._stream.read(2))[0]

    def read_utf(self) -> str:
        length = self.read_ushort()
        data = self._stream.read(length)
        return UTF8.decode(data)

    def read_optional_utf(self) -> Optional[str]:
        if self.read_bool():
            return self.read_utf()
        else:
            return None

class MessageInput(HasStream):
    def __init__(self, stream: Union[BinaryIO, HasStream]) -> None:
        self._stream: Reader = Reader(stream)
        self._flags: int = 0
        self._size: int = 0

    @property
    def stream(self) -> BinaryIO:
        return self._stream.stream

    @property
    def flags(self) -> int:
        return self._flags

    def next(self) -> Optional[Reader]:
        value = self._stream.read_int()
        self._flags = (value & 0xC0000000) >> 30
        self._size = value & 0x3FFFFFFF

        if not self._size:
            return None

        data = self._stream.stream.read(self._size)

        return Reader(io.BytesIO(data))
        
class TrackDecoder:
    """TrackDecoder for track messages."""

    def decode(self, stream: MessageInput):
        """Decode an entire message and return the `Track`."""

        body_reader = stream.next()
        if not body_reader:
            raise ValueError("empty stream")

        version = body_reader.read_byte()

        return {
            "title": body_reader.read_utf(),
            "author": body_reader.read_utf(),
            "length": body_reader.read_long(),
            "identifier": body_reader.read_utf(),
            "isStream": body_reader.read_bool(),
            "uri": body_reader.read_optional_utf(),
            "artworkUrl": None if version not in [0, 3] else body_reader.read_optional_utf(),
            "isrc": None if version != 3 else body_reader.read_optional_utf(),
            "sourceName": body_reader.read_utf(),
            "position": body_reader.read_long()
        }

def decode(data: Union[str, bytes]) -> dict:
    decoded = base64.b64decode(data)
    stream = MessageInput(io.BytesIO(decoded))
    return TrackDecoder().decode(stream)

# Encode
V2_KEYSET = {'title', 'author', 'length', 'identifier', 'isStream', 'uri', 'sourceName', 'position'}
V3_KEYSET = V2_KEYSET | {'artworkUrl', 'isrc'}

class _MissingObj:
    __slots__ = ()

    def __repr__(self):
        return '...'

MISSING: Any = _MissingObj()

class DataWriter:
    __slots__ = ('_buf',)

    def __init__(self):
        self._buf: Final[io.BytesIO] = io.BytesIO()

    def _write(self, data):
        self._buf.write(data)

    def write_byte(self, byte):
        self._buf.write(byte)

    def write_boolean(self, boolean: bool):
        enc = struct.pack('B', 1 if boolean else 0)
        self.write_byte(enc)

    def write_unsigned_short(self, short: int):
        enc = struct.pack('>H', short)
        self._write(enc)

    def write_int(self, integer: int):
        enc = struct.pack('>i', integer)
        self._write(enc)

    def write_long(self, long_value: int):
        enc = struct.pack('>Q', long_value)
        self._write(enc)

    def write_nullable_utf(self, utf_string: Optional[str]):
        self.write_boolean(bool(utf_string))

        if utf_string:
            self.write_utf(utf_string)

    def write_utf(self, utf_string: str):
        utf = utf_string.encode('utf8')
        byte_len = len(utf)

        if byte_len > 65535:
            raise OverflowError('UTF string may not exceed 65535 bytes!')

        self.write_unsigned_short(byte_len)
        self._write(utf)

    def finish(self) -> bytes:
        with io.BytesIO() as track_buf:
            byte_len = self._buf.getbuffer().nbytes
            flags = byte_len | (1 << 30)
            enc_flags = struct.pack('>i', flags)
            track_buf.write(enc_flags)

            self._buf.seek(0)
            track_buf.write(self._buf.read())
            self._buf.close()

            track_buf.seek(0)
            return track_buf.read()
        
def _write_track_common(track: Dict[str, Any], writer: DataWriter):
    writer.write_utf(track['title'])
    writer.write_utf(track['author'])
    writer.write_long(track['length'])
    writer.write_utf(track['identifier'])
    writer.write_boolean(track['isStream'])
    writer.write_nullable_utf(track['uri'])

def encode(
    track: Dict[str, Any],
    source_encoders: Mapping[str, Callable[[DataWriter, Dict[str, Any]], None]] = MISSING
) -> str:
    assert V3_KEYSET <= track.keys()

    writer = DataWriter()
    version = struct.pack('B', 3)
    writer.write_byte(version)
    _write_track_common(track, writer)
    writer.write_nullable_utf(track['artworkUrl'])
    writer.write_nullable_utf(track['isrc'])
    writer.write_utf(track['sourceName'])

    if source_encoders is not MISSING and track['sourceName'] in source_encoders:
        source_encoders[track['sourceName']](writer, track)

    writer.write_long(track['position'])

    enc = writer.finish()
    return base64.b64encode(enc).decode()

# ANSI color codes for console output.
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

def transform_encoded(track_ids: list[str]) -> list[str]:
    encoded_ids = []
    for track_id in track_ids:
        if isinstance(track_id, str):
            try:
                decoded_track = decode(track_id)
                encoded_track = encode(decoded_track)
                encoded_ids.append(encoded_track)
            except Exception:
                # In case of error, skip this track_id without spamming.
                continue
    return encoded_ids

async def connect_db(mongodb_url: str, mongodb_name: str):
    """
    Connect to MongoDB and return the Users collection.
    """
    try:
        client = AsyncIOMotorClient(host=mongodb_url)
        # Verify connection
        await client.server_info()
        print(f"{GREEN}Successfully connected to MongoDB: {mongodb_name}{RESET}")
    except Exception as e:
        print(f"{RED}Unable to connect to MongoDB! Reason: {e}{RESET}")
        exit(1)
        
    # Return the Users collection from the specified database.
    return client[mongodb_name]["Users"]

async def migrate_document(document, collection):
    """
    Process and migrate a single document.
    After processing, updates the document in the database.
    """
    try:
        modified = False  # track changes
        
        # Process playlists, if any.
        playlists = document.get("playlist")
        if playlists:
            for _id, playlist in playlists.items():
                if playlist.get("type") == "playlist":
                    track_ids = playlist.get('tracks', [])
                    if track_ids:
                        playlist["tracks"] = transform_encoded(track_ids)
                        modified = True
        
        # Process history, if any.
        history = document.get("history")
        if history:
            document["history"] = transform_encoded(history)
            modified = True
        
        # Only update the document if it was modified.
        if modified:
            await collection.update_one({"_id": document["_id"]}, {"$set": document})
    except Exception as e:
        await collection.delete_one({"_id": document["_id"]})
        print(f"{RED}Failed to migrate document with _id: {document.get('_id')}. Deleted from the collection. Error: {e}{RESET}")

def update_progress_bar(progress: float, bar_length: int = 40):
    # Calculate number of filled segments.
    filled_length = int(round(bar_length * progress))
    bar = '=' * filled_length + '-' * (bar_length - filled_length)
    percent = round(progress * 100, 1)
    sys.stdout.write(f"\r{BLUE}Progress: [{bar}] {percent}%{RESET}")
    sys.stdout.flush()

async def migrate_documents(collection):
    """
    Loop through all documents in the collection and migrate them.
    Displays a progress bar during migration.
    """
    total_docs = await collection.count_documents({})
    if total_docs == 0:
        print(f"\n{YELLOW}No documents found to migrate.{RESET}")
        return

    cursor = collection.find({})
    processed = 0
    async for document in cursor:
        await migrate_document(document, collection)
        processed += 1
        update_progress_bar(processed / total_docs)
    
    # End the progress bar line.
    sys.stdout.write("\n")

async def load_settings():
    """
    Load MongoDB settings from settings.json in the root directory.
    """
    settings_file = os.path.join(os.getcwd(), "settings.json")
    if not os.path.exists(settings_file):
        print(f"{RED}Settings file not found at {settings_file}{RESET}")
        exit(1)
    try:
        with open(settings_file, "r") as f:
            settings = json.load(f)
            mongodb_url = settings.get("mongodb_url")
            mongodb_name = settings.get("mongodb_name")
            if not mongodb_url or not mongodb_name:
                raise ValueError("Missing mongodb_url or mongodb_name in settings.json")
            return mongodb_url, mongodb_name
    except Exception as e:
        print(f"{RED}Error reading settings.json: {e}{RESET}")
        exit(1)

async def main():
    # Load settings from settings.json.
    mongodb_url, mongodb_name = await load_settings()

    collection = await connect_db(mongodb_url, mongodb_name)
    await migrate_documents(collection)
    print(f"{GREEN}Completed migration process.{RESET}")

if __name__ == "__main__":
    asyncio.run(main())