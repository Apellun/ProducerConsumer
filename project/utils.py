import logging
import os.path
import sys
from collections import namedtuple
from multiprocessing.queues import Queue
from typing import List, Optional

import _queue
from pylibdmtx.pylibdmtx import decode
import cv2
import fitz
import numpy as np
from pymupdf import Page, Pixmap

from project.config import OUTPUT_FILES_PATH, LOGGING_LEVEL, INPUT_FILES_PATH

PageTuple = namedtuple(
    'PageTuple', 'pixmap, parent, number'
)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(LOGGING_LEVEL)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(
        logging.Formatter(
            fmt='%(asctime)s [%(name)s][%(levelname)s]: %(message)s',
            datefmt='%d.%m.%Y %H:%M:%S'
        )
    )
    console_handler.setLevel(LOGGING_LEVEL)

    logger.addHandler(console_handler)
    return logger


def get_doc_paths() -> List[str]:
    os.makedirs(INPUT_FILES_PATH, exist_ok=True)
    return [
        str(INPUT_FILES_PATH / doc) for doc in os.listdir(INPUT_FILES_PATH)
        if doc.endswith('.pdf')
    ]


def get_doc_pages(input_file_path: str) -> List[Optional[Page]]:
    document = fitz.open(input_file_path)
    return [page for page in document]


def get_page_tuple(page: fitz.Page, doc_path: str) -> PageTuple:
    pixmap = page.get_pixmap()
    img = convert_pixmap_to_ndarray(pixmap)
    page = PageTuple(
        pixmap=img, parent=str(doc_path), number=page.number
    )
    return page


def convert_pixmap_to_ndarray(img: Pixmap) -> np.ndarray:
    image_data = img.samples
    image = np.frombuffer(image_data, dtype=np.uint8).reshape(
        (img.h, img.w, img.n)
    )

    if img.n == 4:
        image = cv2.cvtColor(image, cv2.COLOR_RGBA2GRAY)
    return image


def decode_page(page) -> List[Optional[str]]:
    pixmap = page.get_pixmap()

    img = convert_pixmap_to_ndarray(pixmap)
    decoded_codes = decode(img)

    return [str(decoded_code[0], 'utf-8') for decoded_code in decoded_codes]


def get_output_file_path(doc: str) -> str:
    doc_name = doc.split('\\')[-1]
    return OUTPUT_FILES_PATH / doc_name.replace("pdf", "csv")


def write_codes(output_file_path: str, codes: List[str]) -> None:
    os.makedirs(OUTPUT_FILES_PATH, exist_ok=True)

    with open(output_file_path, 'a', encoding="utf-8") as f:
        for code in codes:
            f.write(f"{code}\n")


def queue_to_iterable(queue: Queue) -> List:
    result = []

    while True:
        try:
            i = queue.get(block=False)
            result.append(i)
        except _queue.Empty:
            break

    return result