use tonic::{Request, Response, Status};

use crate::pb::payments::{payment_service_server::{PaymentService, PaymentServiceServer}, PingRequest, PingResponse};

#[derive(Default)]
pub struct PaymentSvc;

#[tonic::async_trait]
impl PaymentService for PaymentSvc {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let message = request.into_inner().message;
        let reply = PingResponse { message };
        Ok(Response::new(reply))
    }
}

pub fn service() -> PaymentServiceServer<PaymentSvc> {
    PaymentServiceServer::new(PaymentSvc::default())
}
