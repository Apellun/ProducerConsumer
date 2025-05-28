from datetime import datetime

from project.config import INPUT_FILES_PATH
from project import utils


def main():
    # последовательная обработка
    logger = utils.get_logger("SimpleAlgorithm")
    timestamp = datetime.now()

    docs = utils.get_doc_paths()

    for doc in docs:
        pages = utils.get_doc_pages(INPUT_FILES_PATH / doc)
        output_path = utils.get_output_file_path(doc)
        doc_codes = []

        for page in pages:
            codes = utils.decode_page(page)
            doc_codes += codes

        utils.write_codes(output_path, doc_codes)
        logger.info(f'В документе {doc} найдено кодов: {len(doc_codes)}.')

    logger.info(f'Обработка завершена за {datetime.now() - timestamp} (ч-м-с-мс).')
    # отработал за за 0:15:05.887543


if __name__ == "__main__":
    main()