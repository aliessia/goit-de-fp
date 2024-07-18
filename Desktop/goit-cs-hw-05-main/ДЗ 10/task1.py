import asyncio
import os
from aiofile import AIOFile
from aiofiles import os as aio_os
import argparse
import logging


parser = argparse.ArgumentParser(description="Asynchronously sort files by extension")
parser.add_argument('source_folder', type=str, help='Source folder to read files from')
parser.add_argument('output_folder', type=str, help='Output folder to save sorted files')
args = parser.parse_args()

source_folder = args.source_folder
output_folder = args.output_folder

async def read_folder(folder):
    files = []
    for root, dirnames, filenames in os.walk(folder):
        for filename in filenames:
            files.append(os.path.join(root, filename))
        for dirname in dirnames:
            subfolder_files = await read_folder(os.path.join(root, dirname))
            files.extend(subfolder_files)
    return files

async def copy_file(file, output_folder):
    ext = os.path.splitext(file)[1][1:]  
    target_folder = os.path.join(output_folder, ext)
    target_file = os.path.join(target_folder, os.path.basename(file))
    
    await aio_os.makedirs(target_folder, exist_ok=True)
    async with AIOFile(file, 'rb') as src_file:
        content = await src_file.read()
    async with AIOFile(target_file, 'wb') as dst_file:
        await dst_file.write(content)


logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

async def main(source_folder, output_folder):
    try:
        files = await read_folder(source_folder)
        await asyncio.gather(*(copy_file(file, output_folder) for file in files))
    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main(source_folder, output_folder))
