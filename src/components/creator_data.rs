use crate::providers::TokenChainData;

pub fn creator_address(chain: &TokenChainData) -> Option<String> {
    chain.creator_address.clone()
}
