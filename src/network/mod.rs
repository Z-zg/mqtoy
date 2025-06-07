use crate::coordinator::Coordinator;
use crate::storage::{Message, MessageStatus};
use crate::transaction::TransactionManager;
use std::net::SocketAddr;
use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;

pub mod proto {
    tonic::include_proto!("mq");
}

#[derive(Debug)]
pub struct MQService {
    coordinator: Coordinator,
    transaction_manager: TransactionManager,
}

#[tonic::async_trait]
impl proto::mq_server::Mq for MQService {
    async fn send_message(
        &self,
        request: Request<proto::SendMessageRequest>,
    ) -> Result<Response<proto::SendMessageResponse>, Status> {
        let req = request.into_inner();
        let message_id = self
            .transaction_manager
            .prepare_message(req.topic, req.body)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::SendMessageResponse {
            message_id: message_id.to_string(),
        }))
    }

    async fn commit_message(
        &self,
        request: Request<proto::CommitMessageRequest>,
    ) -> Result<Response<proto::CommitMessageResponse>, Status> {
        let req = request.into_inner();
        let message_id = Uuid::parse_str(&req.message_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        self.transaction_manager
            .commit_message(message_id)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::CommitMessageResponse {}))
    }

    async fn rollback_message(
        &self,
        request: Request<proto::RollbackMessageRequest>,
    ) -> Result<Response<proto::RollbackMessageResponse>, Status> {
        let req = request.into_inner();
        let message_id = Uuid::parse_str(&req.message_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        self.transaction_manager
            .rollback_message(message_id)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::RollbackMessageResponse {}))
    }
}

pub async fn start_server(
    addr: SocketAddr,
    coordinator: Coordinator,
    transaction_manager: TransactionManager,
) -> Result<(), Box<dyn std::error::Error>> {
    let service = MQService {
        coordinator,
        transaction_manager,
    };

    Server::builder()
        .add_service(proto::mq_server::MqServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
} 