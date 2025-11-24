use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use thiserror::Error;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Streaming};

use crate::config::AppConfig;
use crate::proto::solana_corecast::{
    core_cast_client::CoreCastClient, AddressFilter, DexTradeStreamMessage, SubscribeTradesRequest,
};
use crate::protocol::{ProtocolDefinition, ProtocolRegistry};

#[derive(Clone)]
pub struct CorecastSubscriber {
    config: Arc<AppConfig>,
}

impl CorecastSubscriber {
    pub fn new(config: Arc<AppConfig>) -> Self {
        Self { config }
    }

    pub async fn subscribe_trades(&self) -> Result<Streaming<DexTradeStreamMessage>> {
        let mut client = self.connect().await?;
        let mut request = Request::new(self.build_trades_request());
        let api_key = MetadataValue::try_from(self.config.corecast.api_key.as_str())
            .context("invalid CoreCast API key header")?;
        request.metadata_mut().insert("x-api-key", api_key);

        let response = client
            .dex_trades(request)
            .await
            .context("CoreCast DexTrades subscription failed")?;
        Ok(response.into_inner())
    }

    async fn connect(&self) -> Result<CorecastClient> {
        let endpoint = Endpoint::from_shared(self.config.corecast.endpoint.clone())
            .context("invalid CoreCast endpoint")?
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .connect_timeout(Duration::from_secs(10));

        let channel = endpoint
            .connect()
            .await
            .context("failed to connect CoreCast gRPC endpoint")?;
        Ok(CoreCastClient::new(channel))
    }

    fn build_trades_request(&self) -> SubscribeTradesRequest {
        SubscribeTradesRequest {
            program: if self.config.corecast.program_addresses.is_empty() {
                None
            } else {
                Some(AddressFilter {
                    addresses: self.config.corecast.program_addresses.clone(),
                })
            },
            pool: None,
            token: None,
            trader: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Opportunity {
    pub slot: u64,
    pub signature: String,
    pub program: String,
    pub protocol: Arc<ProtocolDefinition>,
    pub base_mint: String,
    pub quote_mint: String,
    pub buy_amount: u64,
    pub sell_amount: u64,
    pub pool_address: Option<String>,
}

impl Opportunity {
    pub fn from_trade(
        message: DexTradeStreamMessage,
        registry: &ProtocolRegistry,
    ) -> std::result::Result<Self, OpportunityError> {
        let slot = message
            .block
            .ok_or(OpportunityError::MissingField("block"))?
            .slot;
        let transaction = message
            .transaction
            .ok_or(OpportunityError::MissingField("transaction"))?;
        if transaction.signature.is_empty() {
            return Err(OpportunityError::MissingField("transaction.signature"));
        }
        let signature = encode_bs58(&transaction.signature);
        let trade = message
            .trade
            .ok_or(OpportunityError::MissingField("trade"))?;
        let dex = trade
            .dex
            .as_ref()
            .ok_or(OpportunityError::MissingField("trade.dex"))?;
        if dex.program_address.is_empty() {
            return Err(OpportunityError::MissingField("trade.dex.program_address"));
        }
        let program = encode_bs58(&dex.program_address);
        let protocol = registry
            .resolve(&program)
            .ok_or_else(|| OpportunityError::UnknownProtocol(program.clone()))?;
        let market = trade
            .market
            .ok_or(OpportunityError::MissingField("market"))?;
        let base_currency = market
            .base_currency
            .ok_or(OpportunityError::MissingField("market.base_currency"))?;
        let quote_currency = market
            .quote_currency
            .ok_or(OpportunityError::MissingField("market.quote_currency"))?;
        let base_mint = encode_bs58(&base_currency.mint_address);
        let quote_mint = encode_bs58(&quote_currency.mint_address);
        let pool_address = match encode_bs58(&market.market_address) {
            addr if addr.is_empty() => None,
            addr => Some(addr),
        };
        let buy_amount = trade
            .buy
            .as_ref()
            .map(|side| side.amount)
            .unwrap_or_default();
        let sell_amount = trade
            .sell
            .as_ref()
            .map(|side| side.amount)
            .unwrap_or_default();

        Ok(Self {
            slot,
            signature,
            program,
            protocol,
            base_mint,
            quote_mint,
            buy_amount,
            sell_amount,
            pool_address,
        })
    }
}

#[derive(Debug, Error)]
pub enum OpportunityError {
    #[error("missing field {0}")]
    MissingField(&'static str),
    #[error("unknown protocol for program {0}")]
    UnknownProtocol(String),
}

fn encode_bs58(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        String::new()
    } else {
        bs58::encode(bytes).into_string()
    }
}

pub type CorecastClient = CoreCastClient<Channel>;
