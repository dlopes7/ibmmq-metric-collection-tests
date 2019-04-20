import json
import logging
from contextlib import contextmanager

from concurrent.futures import ThreadPoolExecutor
import functools
import threading
import time

import pymqi
from pymqi import CMQC, CMQCFC

MAX_SPLITS = 100

MQPL_PLATFORMS = {
    1: 'z/OS',
    2: 'OS/2',
    3: 'AIX/Unix',
    4: 'IBM i Series',
    5: 'Microoft Windows',
    11: 'Microsoft Windows NT',
    12: 'OpenVMS',
    13: 'HP Integrity NonStop Server',
    15: 'Generic Virtual Machine',
    23: 'z/TPF',
    27: 'z/VSE'
}


def exception_logger(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception as e:
            args[0].logger.exception(f'Error executing {function.__name__}: "{e}"')
            raise

    return wrapper


def time_it(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        start = time.time()
        ret = function(*args, **kwargs)
        delta = time.time() - start

        args[0].logger.info(f'Execution of {function.__name__} took {delta:.3f} seconds')

        return ret

    return wrapper


class MQMConnector(object):

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

        self.group = config['group']

        self.host_port = config['host_port'].strip()
        self.queue_nanager = config['queue_manager'].strip()
        self.channels = config['channel'].strip()
        self.server_channel = config['server_connection'].strip()
        self.listeners = config['listener'].strip()
        self.key_repo_location = config['key_repo_location'].strip()
        self.cipher_spec = config['cipher_spec'].strip()
        self.exclude_system = config['exclude_system']
        self.reset_q_stats = config['reset_q_stats']

        self.last_message_data, self.last_message_time = None, None

        # sself.queues, self.channels, self.listeners = set(), set(), set()
        self.current_queue_depth, max_queue_depth = None, None

        self.qmgr = None

        # We will update this class from multiple threads, we need a lock
        self.lock = threading.RLock()

        self.queue_names_cache = []
        self.channel_names_cache = []

        self.properties_to_report = {}
        self.events_to_report = []
        self.metrics_to_report = []
        self.endpoints_to_report = []

        self.reorganize_queues()
        self.get_host_ips_and_ports()

    def reorganize_queues(self):
        self.queues = self.config['queues'].split(',')
        self.queues.sort(key=lambda queue: '*' in queue)

    def get_host_ips_and_ports(self):

        for host in self.host_port.split(','):
            ip, port = host.split('(')
            port = port[:-1]

            self.endpoints_to_report.append({'ip': ip, 'port': port})

    def get_connection(self):
        queue_manager = self.config['queue_manager']

        connector_descriptor = pymqi.CD()
        connector_descriptor.ChannelName = self.server_channel
        connector_descriptor.ConnectionName = self.host_port
        connector_descriptor.ChannelType = CMQC.MQCHT_CLNTCONN
        connector_descriptor.TransportType = CMQC.MQXPT_TCP
        connector_descriptor.WaitInterval = 30 * 60

        qmgr = pymqi.QueueManager(None)
        qmgr.connect_with_options(queue_manager, connector_descriptor)
        return qmgr

    @contextmanager
    def query_qmgr(self, *args, **kwds):
        conn: pymqi.QueueManager = self.get_connection()
        try:
            yield conn
        finally:
            conn.disconnect()

    @exception_logger
    @time_it
    def report_version(self):
        with self.query_qmgr() as conn:

            version = conn.inquire(CMQC.MQIA_COMMAND_LEVEL)
            platform = MQPL_PLATFORMS[conn.inquire(CMQC.MQIA_PLATFORM)]
            max_priority = conn.inquire(CMQC.MQIA_MAX_PRIORITY)

            version_property = f'IBM MQ {".".join(str(version)[:2])} on {platform}'

            self.logger.info(f'Got properties: Version: "{version_property}", Max Priority: "{max_priority}"')

            with self.lock:
                self.properties_to_report['Version'] = version_property
                self.properties_to_report['Max Priority'] = max_priority

    @exception_logger
    @time_it
    def report_dead_letter(self):
        with self.query_qmgr() as conn:
            dead_letter_queue_name = conn.inquire(CMQC.MQCA_DEAD_LETTER_Q_NAME).decode().strip()
            if dead_letter_queue_name:
                dead_letter_queue = pymqi.Queue(conn, dead_letter_queue_name)
                dead_letter_queue_depth = dead_letter_queue.inquire(CMQC.MQIA_CURRENT_Q_DEPTH)

                self.logger.info(f'Got {dead_letter_queue_depth} messages on dead letter queue {dead_letter_queue_name} for queue manager {self.queue_nanager}')

                if dead_letter_queue_depth:

                    with self.lock:
                        self.events_to_report.append({
                            'title': f'Dead Letter Queue on {self.queue_nanager} has messages',
                            'Description': f'There are {dead_letter_queue_depth} messages on the Dead Letter Queue {dead_letter_queue_name} for the Queue Manager {self.queue_nanager}'
                        })

    @exception_logger
    @time_it
    def get_active_channels(self):
        with self.query_qmgr() as conn:
            args = {
                CMQCFC.MQCACH_CHANNEL_NAME: '*',
                CMQCFC.MQIACH_CHANNEL_INSTANCE_ATTRS: CMQCFC.MQCACH_CHANNEL_NAME,
                CMQCFC.MQIACH_CHANNEL_INSTANCE_TYPE: CMQC.MQOT_CURRENT_CHANNEL
            }

            active_channels = pymqi.PCFExecute(conn).MQCMD_INQUIRE_CHANNEL_STATUS(args)

            with self.lock:
                self.metrics_to_report.append(
                    {
                        'key': 'queueManagerActiveChannels',
                        'type': 'absolute',
                        'value': len(active_channels)
                    }
                )

    @exception_logger
    @time_it
    def refresh_queue_names(self):
        self.logger.info('Refreshing queue names')
        with self.query_qmgr() as conn:
            args = {
                CMQC.MQCA_Q_NAME: '*',
            }

            queue_names = [queue_name.strip() for queue_name in
                           pymqi.PCFExecute(conn).MQCMD_INQUIRE_Q_NAMES(args)[0][CMQCFC.MQCACF_Q_NAMES]]

            with self.lock:
                self.queue_names_cache = queue_names

    @exception_logger
    @time_it
    def refresh_channel_names(self):
        self.logger.info('Refreshing channel names')
        with self.query_qmgr() as conn:
            args = {
                CMQCFC.MQCACH_CHANNEL_NAME: '*',
            }

            channel_names = [channel_name.strip() for channel_name in
                             pymqi.PCFExecute(conn).MQCMD_INQUIRE_CHANNEL_NAMES(args)[0][CMQCFC.MQCACH_CHANNEL_NAMES]]

            with self.lock:
                self.channel_names_cache = channel_names

    @exception_logger
    @time_it
    def get_metrics_listeners(self):
        with self.query_qmgr() as conn:
            args = {
                CMQCFC.MQCACH_LISTENER_NAME: '*',
            }

            listeners = [{'name': listener[CMQCFC.MQCACH_LISTENER_NAME], 'status': listener[CMQCFC.MQIACH_LISTENER_STATUS]} for listener in pymqi.PCFExecute(conn).MQCMD_INQUIRE_LISTENER_STATUS(args)]

            with self.lock:
                for listener in listeners:
                    self.metrics_to_report.append({
                        'key': 'channelListenerStatus',
                        'type': 'absolute',
                        'value': listener['status'],
                        'dimeonsions': {'Listener': listener['name']}
                    })


    @exception_logger
    @time_it
    def get_metrics_queues(self):
        with self.query_qmgr() as conn:
            args = {
                CMQC.MQCA_Q_NAME: '*',
            }

            queue_metrics = {
                'CMQCFC.MQCMD_INQUIRE_Q': {

                    CMQC.MQIA_CURRENT_Q_DEPTH:      {'key': 'queueDepth'},
                    CMQC.MQIA_INHIBIT_GET:          {'key': 'queueInhibitGet'},
                    CMQC.MQIA_INHIBIT_PUT:          {'key': 'queueInhibitPut'},
                    CMQC.MQIA_MAX_Q_DEPTH:          {'key': 'queueMaxQueueDepth'},
                    CMQC.MQIA_OPEN_INPUT_COUNT:     {'key': 'queueOpenInputCount'},
                    CMQC.MQIA_OPEN_OUTPUT_COUNT:    {'key': 'queueOpenOutputCount'},
                },
                'MQCMD_INQUIRE_Q_STATUS': {
                    CMQCFC.MQIACF_OLDEST_MSG_AGE:   {'key': 'queueOldestMessageAge'},
                    CMQCFC.MQIACF_UNCOMMITTED_MSGS: {'key': 'queueUncommittedMessages'},
                    CMQCFC.MQCACF_LAST_GET_DATE:    {'key': 'queueLastGetDate'},
                    CMQCFC.MQCACF_LAST_GET_TIME:    {'key': 'queueLastGetTime'},
                    CMQCFC.MQCACF_LAST_PUT_DATE:    {'key': 'queueLastPutDate'},
                    CMQCFC.MQCACF_LAST_PUT_TIME:    {'key': 'queueLastPutTime'},
                    CMQC.MQIA_TIME_SINCE_RESET:     {'key': 'queueTimeSinceReset'},
                }

            }

            local_metrics = []

            for command in queue_metrics:

                for queue in getattr(pymqi.PCFExecute(conn), command)(args):
                    queue_name = queue[CMQC.MQCA_Q_NAME].strip()

                    for attribute, properties in queue_metrics[command].items():
                        if attribute in queue.keys():

                            local_metrics.append(
                                {
                                    'key': properties["key"],
                                    'value': queue[attribute],
                                    'dimensions': {'Queue': queue_name},
                                    'type': 'absolute'
                                })

            with self.lock:
                self.metrics_to_report.extend(local_metrics)

    @exception_logger
    @time_it
    def get_metrics_channels(self):
        with self.query_qmgr() as conn:
            args = {
                CMQCFC.MQCACH_CHANNEL_NAME: '*',
            }

            channel_metrics = {
                'CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS': {

                    CMQCFC.MQIACH_CHANNEL_STATUS:         {'key': 'channelStatus'},
                    CMQCFC.MQIACH_MSGS:                   {'key': 'channelMessages'},
                    CMQCFC.MQIACH_BYTES_SENT:             {'key': 'channelBytesSent'},
                    CMQCFC.MQIACH_BYTES_RECEIVED:         {'key': 'channelBytesReceived'},
                    CMQCFC.MQIACH_BUFFERS_SENT:           {'key': 'channelBuffersSent'},
                    CMQCFC.MQIACH_BUFFERS_RECEIVED:       {'key': 'channelBuffersReceived'},
                    CMQCFC.MQCACH_LAST_MSG_DATE:          {'key': 'channelLastMsgDate'},
                    CMQCFC.MQCACH_LAST_MSG_TIME:          {'key': 'channelLastMsgTime'},
                    CMQCFC.MQIACH_CURRENT_SHARING_CONVS:  {'key': 'channelCurrentSharingConvs'},

                },
            }

            local_metrics = []

            for command in channel_metrics:

                for channel in getattr(pymqi.PCFExecute(conn), command)(args):
                    channel_name = channel[CMQCFC.MQCACH_CHANNEL_NAME].strip()

                    for attribute, properties in channel_metrics[command].items():
                        if attribute in channel.keys():

                            local_metrics.append(
                                {
                                    'key': properties["key"],
                                    'value': channel[attribute],
                                    'dimensions': {'Channel': channel_name},
                                    'type': 'absolute'
                                })

            with self.lock:
                self.metrics_to_report.extend(local_metrics)

    @exception_logger
    @time_it
    def get_metrics_queue_managers(self):
        with self.query_qmgr() as conn:

            queue_manager_metrics = {
                'CMQCFC.MQCMD_INQUIRE_Q_MGR_STATUS': {

                    CMQCFC.MQIACF_Q_MGR_STATUS:           {'key': 'queueManagerStatus'},
                    CMQCFC.MQIACF_CONNECTION_COUNT:       {'key': 'queueManagerConnections'},
                },
            }

            local_metrics = []

            for command in queue_manager_metrics:
                for queue_manager in getattr(pymqi.PCFExecute(conn), command)():
                    for attribute, properties in queue_manager_metrics[command].items():
                        if attribute in queue_manager.keys():

                            local_metrics.append(
                                {
                                    'key': properties["key"],
                                    'value': queue_manager[attribute],
                                    'type': 'absolute'
                                })

            with self.lock:
                self.metrics_to_report.extend(local_metrics)

    @exception_logger
    @time_it
    def collect(self):

        with ThreadPoolExecutor(max_workers=20) as e:
            e.submit(self.report_version)
            e.submit(self.report_dead_letter)
            e.submit(self.get_active_channels)
            e.submit(self.refresh_queue_names)
            e.submit(self.refresh_channel_names)

            e.submit(self.get_metrics_listeners)
            e.submit(self.get_metrics_queues)
            e.submit(self.get_metrics_channels)
            e.submit(self.get_metrics_queue_managers)

        self.logger.info(f'Collected {len(self.metrics_to_report)} metrics')
        self.logger.info(f'Collected {len(self.events_to_report)} events')
        self.logger.info(f'Collected {len(self.properties_to_report)} properties')
        self.logger.info(f'Collected {len(self.endpoints_to_report)} endpoints')


def main():
    with open('properties.json', 'r') as f:
        config = json.load(f)
        logger = logging.getLogger(__name__)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(funcName)s - %(message)s ")
        ch.setFormatter(fmt)

        logger.setLevel(logging.INFO)
        logger.addHandler(ch)

        mq = MQMConnector(config, logger)
        mq.collect()

        with open('results.json', 'w') as out:
            results = [mq.endpoints_to_report, mq.properties_to_report, mq.metrics_to_report, mq.events_to_report]
            json.dump(results, out, sort_keys=True, indent=4)


if __name__ == '__main__':
    main()




