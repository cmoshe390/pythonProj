import threading
from socketProducer import SocketProducer
from rabbitPublishFromFile import RabbitPublishFromFile
from socketPublishFromFile import SocketPublishFromFile
from folderListener import Listener
from pika.exchange_type import ExchangeType
from serverSocket import Server
import sys
from rabbitProducer import RabbitProducer
from extractFiles import ExtractFiles


if __name__ == '__main__':

    extractor_files = ExtractFiles()
    exchange = 'exchange1'
    routing_key = ''
    exchange_type = ExchangeType.topic

    _dir = r"C:\dev\integratedSystem\images"
    backup_dir = r"C:\dev\integratedSystem\all_images"
    _chunk_size = 20000
    work_with = sys.argv[1]

    threads = []
    producers = []
    publishers = []
    if work_with == 'rabbit':
        for i in range(1, 3):
            producer = RabbitProducer(exchange, routing_key, exchange_type)
            producers.append(producer)
            publishers.append(RabbitPublishFromFile(i, backup_dir, _chunk_size, producer))
    elif work_with == 'socket':
        server = Server()
        threads.append(threading.Thread(target=server.run))
        for i in range(1, 3):
            producer = SocketProducer(server)
            producers.append(producer)
            publishers.append(SocketPublishFromFile(i, backup_dir, _chunk_size, producer))
    try:
        listener = Listener(_dir, backup_dir, publishers, extractor_files.handler_files)

        for producer in producers:
            threads.append(threading.Thread(target=producer.run))
        threads.append(threading.Thread(target=listener.run))

    except NameError as e:
        print("the parameter in args must be 'rabbit' or 'socket'!")

    for thread in threads:
        thread.start()
