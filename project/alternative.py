from datetime import datetime
from multiprocessing import (
    Queue, Event, Manager, Process,
    freeze_support, Lock, Value
)
from typing import Dict, Optional

import _queue
from pylibdmtx.pylibdmtx import decode

from project import utils

logger = utils.get_logger("PagesToDmtx")


class PagesToDmtx:
    def __init__(self):
        self._docs_queue: Optional[Queue] = None
        self._manager: Optional[Manager] = None
        self._results_dict: Optional[Dict] = None

    @staticmethod
    def _get_docs_queue() -> Queue:
        docs_queue = Queue()
        docs = utils.get_doc_paths()

        logger.info(f"Документов для обработки: {len(docs)}.")

        for doc in docs:
            docs_queue.put(doc)

        return docs_queue

    @staticmethod
    def _form_pages_queue(
        docs_queue: Queue, pages_queue: Queue, finished: Event, counter: Value
    ) -> None:
        while True:
            if not docs_queue.empty():
                try:
                    doc_path = docs_queue.get(timeout=0.1)
                    logger.info(f"Добавляю в очередь страницы документа {doc_path}.")

                    pages = utils.get_doc_pages(doc_path)

                    for page in pages:
                        pages_queue.put(utils.get_page_tuple(page, doc_path))

                    logger.info(
                        f"В очередь обработки добавлены страницы документа {doc_path},"
                        f" всего {len(pages)} шт."
                    )

                    # тут я отслеживаю, что все документы точно обработаны,
                    # прежде, чем сигналить, что очередь больше не будет пополняться
                    counter.value -= 1
                    if counter.value <= 0:
                        finished.set()

                except _queue.Empty:
                    continue

                except Exception as e:
                    logger.exception(e)
                    break

            else:
                break

    @staticmethod
    def _get_page_codes(
        pages_queue: Queue, pages_queue_done: Event, results_dict: Dict[str, str], lock: Lock
    ) -> None:
        while True:
            if not pages_queue.empty():
                try:
                    page = pages_queue.get(timeout=0.1)
                    logger.info(f'Обрабатываю страницу {page.number} документа {page.parent}')

                    decoded_page = decode(page.pixmap)
                    codes = [str(decoded_code[0], 'utf-8') for decoded_code in decoded_page]

                    logger.info(
                        f'На странице {page.number} документа {page.parent}'
                        f' найдено кодов: {len(codes)}'
                    )

                    with lock:
                        if not results_dict.get(page.parent):
                            results_dict[page.parent] = codes
                        else:
                            results_dict[page.parent] += codes

                except _queue.Empty:
                    continue

                except Exception as e:
                    logger.exception(e)
                    break

            else:
                # соответственно, парсинг страниц заканчивается только тогда,
                # когда очередь пуста и больше не будет пополняться
                if pages_queue_done.is_set():
                    break

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
        pages_queue = Queue()
        pages_queue_done = Event()
        counter = manager.Value(typecode=int, value=docs_amount)
        lock = Lock()
        self._results_dict = manager.dict()

        # тут консюмер не дожидается, пока продюсер закончит, а берет работу сразу
        producers = [
            Process(
                target=self._form_pages_queue, args=(
                    self._docs_queue, pages_queue, pages_queue_done, counter
                )) for _ in range(docs_amount)
        ]
        consumers = [
            Process(target=self._get_page_codes, args=(
                pages_queue, pages_queue_done, self._results_dict, lock
            )) for _ in range(docs_amount * 4)
        ]

        for _w in producers + consumers:
            _w.start()
        for _w in consumers:
            _w.join()
        for _w in producers:
            _w.join()

        self._manage_results()
        logger.info(f'Обработка завершена за {datetime.now() - timestamp} (ч-м-с-мс).')
        # отработал за 0:01:05.224734


if __name__ == '__main__':
    freeze_support()
    manager = PagesToDmtx()
    manager.run()