use std::{collections::BTreeMap, sync::Arc};

use cooplan_definitions_lib::definition::Definition;
use futures_util::StreamExt;
use lapin::{
    message::DeliveryResult,
    options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable, LongString, ShortString},
    Channel, Connection, ConnectionProperties,
};
use tokio::sync::watch::{self, Receiver, Sender};

use crate::error::{Error, ErrorKind};

pub struct RabbitMQInput {
    connection_uri: String,
    amqp_queue_name: String,
    channel: Option<Channel>,
}

impl RabbitMQInput {
    pub fn new(connection_uri: String, amqp_queue_name: String) -> RabbitMQInput {
        RabbitMQInput {
            connection_uri,
            amqp_queue_name,
            channel: None,
        }
    }

    pub async fn connect(&mut self) -> Result<(), Error> {
        let connection_options = ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio);

        match Connection::connect(&self.connection_uri.as_str(), connection_options).await {
            Ok(connection) => match connection.create_channel().await {
                Ok(channel) => {
                    let mut options = QueueDeclareOptions::default();
                    options.durable = true;
                    options.exclusive = false;
                    options.auto_delete = false;

                    let mut map: BTreeMap<ShortString, AMQPValue> = BTreeMap::new();
                    map.insert(
                        ShortString::from("x-queue-type"),
                        AMQPValue::LongString(LongString::from("stream")),
                    );

                    let arguments = FieldTable::from(map);

                    match channel
                        .queue_declare(self.amqp_queue_name.as_str(), options, arguments)
                        .await
                    {
                        Ok(_) => {
                            self.channel = Some(channel);

                            Ok(())
                        }
                        Err(error) => Err(Error::new(
                            ErrorKind::ConnectionFailure,
                            format!("failed to create queue: {}", error),
                        )),
                    }
                }
                Err(error) => Err(Error::new(
                    ErrorKind::ConnectionFailure,
                    format!("failed to connect: {}", error),
                )),
            },
            Err(error) => Err(Error::new(
                ErrorKind::ConnectionFailure,
                format!("failed to connect: {}", error),
            )),
        }
    }

    pub async fn get(&self) -> Result<Definition, Error> {
        let (mut sender, mut receiver): (Sender<Option<Definition>>, Receiver<Option<Definition>>) =
            watch::channel(None);

        let sen = Arc::new(sender);

        match &self.channel {
            Some(channel) => {
                channel.basic_qos(1u16, BasicQosOptions::default()).await;

                let mut map: BTreeMap<ShortString, AMQPValue> = BTreeMap::new();
                map.insert(
                    ShortString::from("x-stream-offset"),
                    AMQPValue::LongString(LongString::from("last")),
                );

                let arguments = FieldTable::from(map);

                let mut options = BasicConsumeOptions::default();
                options.no_ack = false;

                match channel
                    .basic_consume(
                        self.amqp_queue_name.as_str(),
                        "",
                        BasicConsumeOptions::default(),
                        arguments,
                    )
                    .await
                {
                    Ok(mut consumer) => {
                        while let Some(delivery) = consumer.next().await {
                            let delivery = delivery.expect("error in consumer");

                            delivery.ack(BasicAckOptions::default()).await;

                            match std::str::from_utf8(delivery.data.as_slice()) {
                                Ok(definition_data) => {
                                    let definition_result: Result<Definition, serde_json::Error> =
                                        serde_json::from_slice(delivery.data.as_slice());

                                    match definition_result {
                                        Ok(definition) => {
                                            return Ok(definition);
                                        }
                                        Err(error) => {
                                            log::error!(
                                                "failed to deserialize definition from string: {}",
                                                error
                                            );
                                            return Err(Error::new(
                                                ErrorKind::ConsumerFailure,
                                                format!("failed to deserialize definition from string: {}", error),
                                            ));
                                        }
                                    }
                                }
                                Err(error) => {
                                    log::error!("failed to read string from bytes: {}", error);
                                    return Err(Error::new(
                                        ErrorKind::ConsumerFailure,
                                        format!("failed to read string from bytes: {}", error),
                                    ));
                                }
                            }
                        }

                        Err(Error::new(
                            ErrorKind::ConsumerFailure,
                            "no delivery read".to_string(),
                        ))
                    }
                    Err(error) => Err(Error::new(
                        ErrorKind::ConsumerFailure,
                        format!("failed to consume: {}", error),
                    )),
                }
            }
            None => Err(Error::new(
                ErrorKind::NotConnected,
                "tried to set without having a connection".to_string(),
            )),
        }
    }
}
