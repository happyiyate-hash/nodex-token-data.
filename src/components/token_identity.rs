use crate::{models::TokenIdentity, providers::TokenChainData};

pub fn from_chain(mint: &str, chain: &TokenChainData, created_at: Option<String>) -> TokenIdentity {
    TokenIdentity {
        name: chain.name.clone(),
        symbol: chain.symbol.clone(),
        logo_url: chain.logo_url.clone(),
        mint_address: mint.to_owned(),
        creator_address: chain.creator_address.clone(),
        created_at,
    }
}
