from rabbitConsumer import *
from socketConsumer import SocketConsumer
from dlx import *

import threading
import sys

if __name__ == '__main__':

    work_with = sys.argv[1]

    r_k = ['*.jpg', '*.jpeg', '#']
    threads = []
    dlx = ReconnectingDlx()

    threads.append(threading.Thread(target=dlx.run))

    for j in range(1, 4):
        if work_with == 'rabbit':
            # consumer = RabbitConsumer(_id_consumer=j, _exchange='exchange1',
            #                          _queue=f'queue{j}', _routing_key=r_k[j - 1], _exchange_type='topic',
            #                          _producer_to_dlx=dlx)
            consumer = RabbitReconnectingConsumer(_id_consumer=j, _exchange='exchange1',
                                                  _queue=f'queue{j}', _routing_key=r_k[j - 1], _exchange_type='topic',
                                                  _producer_to_dlx=dlx)
        elif work_with == 'socket':
            consumer = SocketConsumer(_id_consumer=j)
        else:
            print("the parameter in args must be 'rabbit' or 'socket'!")

        threads.append(threading.Thread(target=consumer.run))

    for thread in threads:
        thread.start()
