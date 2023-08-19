from uvicorn import run as uvicorn_run
from fastapi import FastAPI, File, UploadFile, BackgroundTasks
import hashlib
import os
from dotenv import load_dotenv
from uuid import uuid4


load_dotenv()

app = FastAPI(
    title='Upload large file',
    description="Part of interview. Made by Alan Amirgalin",
    debug=True,
)

uploaded_chunks_db = {}

UPLOADS_DIR = os.getenv('UPLOADS_DIR')
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE'))


@app.get("/")
def read_root():
    return {"Project name": "Upload large file"}


@app.get("/uuid4")
def get_random_uuid():
    '''Just returns generated uuid4, nothing more'''
    return {'result': uuid4()}


def divide_file_into_chunks(file_path, chunk_size):
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            yield chunk


@app.post("/upload-file")
async def upload_large_file(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None
):
    '''Save the uploaded file temporarily and divided it into chunks'''
    unique_identifier = uuid4()
    temp_file_path = os.path.join(UPLOADS_DIR, f"temp_{unique_identifier}.bin")
    with open(temp_file_path, "wb") as temp_file:
        temp_file.write(file.file.read())

    # Divide the uploaded file into chunks and store them in the background
    background_tasks.add_task(store_chunks, temp_file_path, CHUNK_SIZE, unique_identifier)
    return {"message": "File upload in progress"}


def store_chunks(file_path, chunk_size, identifier: str):
    for chunk_number, chunk in enumerate(divide_file_into_chunks(file_path, chunk_size)):
        md5_hash = hashlib.md5(chunk).hexdigest()
        tag = f"{identifier}_{chunk_number}"
        chunk_path = os.path.join(UPLOADS_DIR, f"{tag}.bin")
        with open(chunk_path, "wb") as chunk_file:
            chunk_file.write(chunk)
        uploaded_chunks_db[(file_path, chunk_number)] = (md5_hash, tag)
    os.remove(file_path)  # Clean up temporary file


@app.get("/uploaded_chunks_db")
async def get_db_info():
    '''Gets the data from a custom cache'''
    result = dict()
    for k, v in uploaded_chunks_db.items():
        result[str(k)] = str(v)
    return result


if __name__ == '__main__':
    os.makedirs(UPLOADS_DIR, exist_ok=True)
    uvicorn_run(app, host="localhost", port=8000, timeout_keep_alive=1000)
