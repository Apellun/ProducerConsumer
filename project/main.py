from datetime import datetime
from multiprocessing import (
    Queue, Manager, Process, Pool
)
from typing import Dict, List, Optional

import _queue
from pylibdmtx.pylibdmtx import decode

from project import utils
from project.utils import PageTuple

logger = utils.get_logger("PagesToDmtx")


class PagesToDmtx:
    def __init__(self):
        self._docs_queue: Optional[Queue] = None
        self._manager: Optional[Manager] = None
        self._results_dict: Optional[Dict] = None

    @staticmethod
    def _get_docs_queue() -> Queue:
        docs_queue = Queue()
        doc_paths = utils.get_doc_paths()

        logger.info(f"Документов для обработки: {len(doc_paths)}.")

        for doc in doc_paths:
            docs_queue.put(doc)

        return docs_queue

    @staticmethod
    def _form_pages_queue(docs_queue: Queue, pages_queue: Queue) -> None:
        while True:
            if not docs_queue.empty():
                try:
                    doc_path = docs_queue.get(timeout=0.1)
                    logger.info(f"Обрабатываю страницы документа {doc_path}.")

                    pages = utils.get_doc_pages(doc_path)

                    for page in pages:
                        pages_queue.put(utils.get_page_tuple(page, doc_path))

                    logger.info(
                        f"В очередь обработки добавлены страницы документа {doc_path},"
                        f" всего {len(pages)} шт."
                    )

                except _queue.Empty:
                    break

                except Exception as e:
                    logger.exception(e)

            else:
                break

    @staticmethod
    def _process_page(page: PageTuple) -> tuple[PageTuple, List[Optional[str]]]:
        logger.info(f'Обрабатываю страницу {page.number} документа {page.parent}')

        decoded_page = decode(page.pixmap)
        codes = [str(decoded_code[0], 'utf-8') for decoded_code in decoded_page]

        logger.info(
            f'На странице {page.number} документа {page.parent}'
            f' найдено кодов: {len(codes)}'
        )

        return page, codes

    def _add_codes_to_results(self, results: List[tuple]) -> None:
        for result in results:
            page, codes = result

            if not self._results_dict.get(page.parent):
                self._results_dict[page.parent] = codes
            else:
                self._results_dict[page.parent] += codes

    def _manage_results(self) -> None:
        results_info = 'Результат:'

        for key, value in self._results_dict.items():
            output_file_path = utils.get_output_file_path(key)
            utils.write_codes(output_file_path, value)
            results_info += f'\nВ документе {key} найдено кодов: {len(value)}.'

        logger.info(results_info)

    def run(self) -> None:
        timestamp = datetime.now()

        self._docs_queue = self._get_docs_queue()
        docs_amount = self._docs_queue.qsize()

        if docs_amount <= 0:
            logger.warning("Нет документов для обработки")
            return

        manager = Manager()
        pages_queue = manager.Queue()
        self._results_dict = manager.dict()

        # я подумала, что последовательный парсинг файлов на страницы тоже
        # может быть "бутылочным горлышком", и его тоже можно попробовать
        # ускорить

        producers = [
            Process(
                target=self._form_pages_queue, args=(self._docs_queue, pages_queue)
            ) for _ in range(docs_amount)
        ]
        for _w in producers:
            _w.start()
            _w.join()

        # в пул уже очередь не передать, только итерируемую коллекцию
        page_queue_list = utils.queue_to_iterable(pages_queue)
        logger.info(f'Формирование страниц закончено, страниц в очереди: {len(page_queue_list)}.')

        pool = Pool()
        pool.map_async(self._process_page, page_queue_list, callback=self._add_codes_to_results)
        pool.close()
        pool.join()

        self._manage_results()

        logger.info(f'Обработка завершена за {datetime.now() - timestamp} (ч-м-с-мс).')
        # отработал за 0:02:40.691270


if __name__ == '__main__':
    manager = PagesToDmtx()
    manager.run()