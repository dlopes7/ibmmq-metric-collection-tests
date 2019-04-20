
import json
import pymqi
import random

from pymqi import CMQCFC, CMQC


with open('properties.json', 'r') as f:
    config = json.load(f)


def get_connection():
    queue_manager = config['queue_manager']

    connector_descriptor = pymqi.CD()
    connector_descriptor.ChannelName = config['server_connection']
    connector_descriptor.ConnectionName = config['host_port']
    connector_descriptor.ChannelType = CMQC.MQCHT_CLNTCONN
    connector_descriptor.TransportType = CMQC.MQXPT_TCP

    qmgr = pymqi.QueueManager(None)
    qmgr.connect_with_options(queue_manager, connector_descriptor)
    return qmgr


def create_channels(conn, number):
    for i in range(number):
        try:
            channel_name = f'MYCHANNEL.{i}'
            channel_type = pymqi.CMQXC.MQCHT_SVRCONN

            print(f'Creating channel {channel_name} {channel_type}')

            args = {pymqi.CMQCFC.MQCACH_CHANNEL_NAME: channel_name,
                    pymqi.CMQCFC.MQIACH_CHANNEL_TYPE: channel_type}

            pcf = pymqi.PCFExecute(conn)
            pcf.MQCMD_CREATE_CHANNEL(args)

        except pymqi.MQMIError as e:
            if 'MQRCCF_OBJECT_ALREADY_EXISTS' in e.errorAsString():
                print(f'Could not create channel, {channel_name} already exists')
            else:
                raise e


def create_queues(conn, number):
    for i in range(number):
        try:
            queue_name = f'MYQUEUE.{i}'
            queue_type = pymqi.CMQC.MQQT_LOCAL
            max_depth = 123456

            print(f'Creating queue {queue_name} {queue_type} {max_depth}')

            args = {pymqi.CMQC.MQCA_Q_NAME: queue_name,
                    pymqi.CMQC.MQIA_Q_TYPE: queue_type,
                    pymqi.CMQC.MQIA_MAX_Q_DEPTH: max_depth}

            pcf = pymqi.PCFExecute(conn)
            pcf.MQCMD_CREATE_Q(args)

        except pymqi.MQMIError as e:
            if 'MQRCCF_OBJECT_ALREADY_EXISTS' in e.errorAsString():
                print(f'Could not create queue, {queue_name} already exists')
            else:
                raise e

def delete_queues(conn, number):
    for i in range(number):
        try:
            queue_name = f'MYQUEUE.{i}'

            print(f'Deleting {queue_name}')

            args = {pymqi.CMQC.MQCA_Q_NAME: queue_name}

            pcf = pymqi.PCFExecute(conn)
            pcf.MQCMD_DELETE_Q(args)

        except pymqi.MQMIError as e:
            print(f'Could not delete queue, {queue_name}')


def create_things(queues=0, channels=0):
    conn = get_connection()

    create_channels(conn, channels)
    create_queues(conn, queues)

    #delete_queues(conn, queues)
    conn.disconnect()


def generate_load(messages):

    conn = get_connection()

    for i in range(messages):
        queue_name = f'MYQUEUE.{random.randint(0, 999)}'
        message = f'This is a random message {random.randint(0, 2**32)}'

        print(f'Inserting {queue_name}: {message}')

        queue = pymqi.Queue(conn, queue_name)
        queue.put(message)
        queue.close()

    conn.disconnect()


def main():
    create_things(queues=1000, channels=1000)
    generate_load(100000)


if __name__ == '__main__':
    main()
