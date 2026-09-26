import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

MY_KEY = f"{SUM_PREFIX}_{ID}"
ALL_KEYS = [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT)]

QUERY_COUNT = 1
REPORT_COUNT = 2
FINAL_EOF = 3

class SumFilter:
    def __init__(self):
        self.fruits_dict_by_client: dict[str, dict[str, fruit_item.FruitItem]] = {}
        self.msg_count_by_client: dict[str, int] = {}

        self.received_reports_by_client: dict[str, dict[str, int]] = {}
        self.expected_totals_by_client: dict[str, int] = {}

        self.lock = threading.Lock()

    def _process_data(self, client_id, fruit, amount):
        with self.lock:
            logging.info(f"Process data for client {client_id}")

            if client_id not in self.fruits_dict_by_client:
                self.fruits_dict_by_client[client_id] = {}
                self.msg_count_by_client[client_id] = 0

            client_dict = self.fruits_dict_by_client[client_id]
            client_dict[fruit] = client_dict.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

            self.msg_count_by_client[client_id] += 1

    def _get_local_count(self, client_id):
        with self.lock:
            return self.msg_count_by_client.get(client_id, 0)

    def _listen_control_messages(self):
        sum_control_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [MY_KEY]
        )
        sum_control_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, ALL_KEYS
        )
        data_output_exchanges = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            for i in range(AGGREGATION_AMOUNT)
        ]

        def on_control_message(message, ack, nack):
            fields = message_protocol.internal.deserialize(message)
            msg_type = fields[0]

            if msg_type == QUERY_COUNT:
                sender_key, client_id = fields[1], fields[2]
                if sender_key == MY_KEY:
                    ack()
                    return
                
                local_count = self._get_local_count(client_id)
                
                sum_control_publisher.send_to(
                    message_protocol.internal.serialize(
                        [REPORT_COUNT, MY_KEY, client_id, local_count]
                    ),
                    routing_key=sender_key
                )

            elif msg_type == REPORT_COUNT:
                sender_key, client_id, count = fields[1], fields[2], int(fields[3])

                with self.lock:
                    if client_id in self.received_reports_by_client:
                        self.received_reports_by_client[client_id][sender_key] = count

                action = self._check_reports_and_proceed(client_id)
                if action:
                    action_type, msg = action
                    sum_control_publisher.send(msg)

            elif msg_type == FINAL_EOF:
                client_id = fields[1]
                self._send_results_to_aggregator(client_id, data_output_exchanges)

            ack()

        sum_control_consumer.start_consuming(on_control_message)

    def _check_reports_and_proceed(self, client_id):
        with self.lock:
            if len(self.received_reports_by_client[client_id]) < SUM_AMOUNT:
                return None

            accumulated_total = sum(self.received_reports_by_client[client_id].values())
            expected_total = self.expected_totals_by_client[client_id]

            if accumulated_total == expected_total:
                msg = message_protocol.internal.serialize([FINAL_EOF, client_id])
                
                del self.received_reports_by_client[client_id]
                del self.expected_totals_by_client[client_id]
                return ("BROADCAST_FINAL_EOF", msg)
            else:
                logging.warning(
                    f"Not all messages for {client_id} received: {accumulated_total}/{expected_total}. Retrying"
                )
                self.received_reports_by_client[client_id] = {
                    MY_KEY: self.msg_count_by_client.get(client_id, 0)
                }
                msg = message_protocol.internal.serialize([QUERY_COUNT, MY_KEY, client_id])
                return ("BROADCAST_QUERY_COUNT", msg)

    def _process_eof(self, client_id, expected_total, sum_control_publisher):
        expected_total = int(expected_total)
        logging.info(f"EOF received for {client_id}. Expected total: {expected_total}")

        with self.lock:
            my_count = self.msg_count_by_client.get(client_id, 0)
            self.expected_totals_by_client[client_id] = expected_total
            self.received_reports_by_client[client_id] = {
                MY_KEY: my_count,
            }

        action = self._check_reports_and_proceed(client_id)

        if action:
            _, msg = action
            sum_control_publisher.send(msg)
        else:
            sum_control_publisher.send(
                message_protocol.internal.serialize([QUERY_COUNT, MY_KEY, client_id])
            )

    def _send_results_to_aggregator(self, client_id, data_output_exchanges):
        with self.lock:
            client_dict = self.fruits_dict_by_client.pop(client_id, {})
            self.msg_count_by_client.pop(client_id, None)

        logging.info(f"Sending accumulated totals for client {client_id} to Aggregator")

        for final_fruit_item in client_dict.values():
            for data_output_exchange in data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))


    def process_data_messsage(self, message, ack, nack, sum_control_publisher):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            client_id, fruit, amount = fields
            self._process_data(client_id, fruit, amount)
        else:
            client_id, expected_total = fields
            self._process_eof(client_id, expected_total, sum_control_publisher)
        ack()

    def start(self):
        control_thread = threading.Thread(target=self._listen_control_messages)
        control_thread.start()

        input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        main_sum_control_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, ALL_KEYS
        )

        def on_data_message(message, ack, nack):
            self.process_data_messsage(message, ack, nack, main_sum_control_publisher)

        input_queue.start_consuming(on_data_message)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
