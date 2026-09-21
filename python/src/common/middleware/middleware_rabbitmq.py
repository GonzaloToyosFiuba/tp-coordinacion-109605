import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name

        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host)
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"No se pudo conectar a RabbitMQ en {host}: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error al inicializar la cola en RabbitMQ: {e}")

    def start_consuming(self, on_message_callback):
        def internal_callback(ch, method, properties, body):
            delivery_tag = method.delivery_tag

            def ack():
                ch.basic_ack(delivery_tag=delivery_tag)

            def nack(requeue=True):
                ch.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=internal_callback)
            self.channel.start_consuming()
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError(f"Conexión perdida en el consumidor: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error interno en consumidor: {e}")

    def stop_consuming(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(f"Error al detener el consumo: {e}")
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error inesperado al detener el consumo: {e}")

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                )
            )
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexión al enviar mensaje: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error interno de Pika al enviar: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error inesperado al enviar mensaje: {e}")

    def close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(f"Error de Pika al cerrar la conexión: {e}")
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error inesperado al cerrar la conexión: {e}")


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()

            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"No se pudo conectar a RabbitMQ en {host}: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error al inicializar el exchange en RabbitMQ: {e}")

    def send(self, message):
        try:
            for key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=key, body=message)
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError(f"Error de conexión al enviar mensaje: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error interno al publicar en el enchange: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error inesperado al enviar mensaje: {e}")

    def start_consuming(self, on_message_callback):
        def internal_callback(ch, method, properties, body):
            delivery_tag = method.delivery_tag

            def ack():
                ch.basic_ack(delivery_tag=delivery_tag)

            def nack(requeue=True):
                ch.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

            on_message_callback(body, ack, nack)

        try:
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue

            for key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=key)

            self.channel.basic_qos(prefetch_count=1)

            self.channel.basic_consume(queue=queue_name, on_message_callback=internal_callback)
            self.channel.start_consuming()
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError(f"Conexión perdida en el consumidor: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error interno en consumidor: {e}")

    def stop_consuming(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(f"Error al detener el consumo: {e}")
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error inesperado al detener el consumo: {e}")

    def close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(f"Error de Pika al cerrar la conexión: {e}")
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error inesperado al cerrar la conexión: {e}")