mod services;
pub mod pb;

use tonic::transport::Server;
use services::payment::service;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    println!("connector-rust listening on {}", addr);

    Server::builder()
        .add_service(service())
        .serve(addr)
        .await?;
    Ok(())
}
