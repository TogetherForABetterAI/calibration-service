import grpc
import logging
from concurrent import futures
from core.client_manager import ClientManager
from src.pb.new_client_service import (
    new_client_service_pb2,
    new_client_service_pb2_grpc,
)


class ClientNotificationServiceServicer(
    new_client_service_pb2_grpc.ClientNotificationServiceServicer
):
    def __init__(self, client_manager: ClientManager):
        super().__init__()
        self.client_manager = client_manager
        logging.info("ClientNotificationServiceServicer initialized")

    def NotifyNewClient(self, request, context):

        client_id = request.client_id
        outputs_queue_calibration = f"outputs_queue_calibration_client_{client_id}"
        inputs_queue_calibration = f"inputs_queue_calibration_client_{client_id}"

        logging.info(
            f"Received NotifyNewClient request for client {client_id} with queues: calibration={outputs_queue_calibration}, inter_connection={inputs_queue_calibration}"
        )

        if (
            not client_id
            or not outputs_queue_calibration
            or not inputs_queue_calibration
        ):
            error_msg = "Missing required parameters: client_id, outputs_queue_calibration, or inputs_queue_calibration"
            logging.error(error_msg)
            return new_client_service_pb2.NewClientResponse(
                status="ERROR", message=error_msg
            )

        try:
            self.client_manager.register_client(
                client_id=client_id,
                outputs_queue_calibration=outputs_queue_calibration,
                inputs_queue_calibration=inputs_queue_calibration,
            )
        except Exception as e:
            error_msg = f"Failed to register client {client_id}: {str(e)}"
            logging.error(error_msg, exc_info=True)
            return new_client_service_pb2.NewClientResponse(
                status="ERROR", message=error_msg
            )

        success_msg = f"Successfully registered client {client_id}"
        logging.info(success_msg)
        return new_client_service_pb2.NewClientResponse(
            status="OK", message=success_msg
        )

    def HealthCheck(self, request, context):
        try:
            active_clients = self.client_manager.get_active_clients()
            client_count = len(active_clients)
            health_msg = f"Service is healthy. Active clients: {client_count}"
            logging.debug(health_msg)
            return new_client_service_pb2.HealthCheckResponse(
                status="SERVING", message=health_msg
            )
        except Exception as e:
            error_msg = f"Health check failed: {str(e)}"
            logging.error(error_msg)
            return new_client_service_pb2.HealthCheckResponse(
                status="NOT_SERVING", message=error_msg
            )


class GrpcServer:
    def __init__(self, client_manager: ClientManager, port: int = 50051):
        self.client_manager = client_manager
        self.port = port
        self.server = None

    def start(self):
        try:
            self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            servicer = ClientNotificationServiceServicer(self.client_manager)
            new_client_service_pb2_grpc.add_ClientNotificationServiceServicer_to_server(
                servicer, self.server
            )
            server_address = f"[::]:{self.port}"
            self.server.add_insecure_port(server_address)
            self.server.start()
            logging.info(f"gRPC server started on port {self.port}")
        except Exception as e:
            logging.error(f"Failed to start gRPC server: {e}")
            raise

    def stop(self):
        if self.server:
            try:
                self.server.stop(grace=5)
                logging.info("gRPC server stopped")
            except Exception as e:
                logging.error(f"Error stopping gRPC server: {e}")

    def wait_for_termination(self):
        if self.server:
            self.server.wait_for_termination()
