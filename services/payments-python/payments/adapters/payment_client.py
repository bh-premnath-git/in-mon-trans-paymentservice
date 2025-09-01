import grpc
from ..pb import payments_pb2, payments_pb2_grpc

async def ping(message: str) -> str:
    async with grpc.aio.insecure_channel("connector-rust:50051") as channel:
        stub = payments_pb2_grpc.PaymentServiceStub(channel)
        response = await stub.Ping(payments_pb2.PingRequest(message=message))
    return response.message
