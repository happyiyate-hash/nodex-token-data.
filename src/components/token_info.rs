use crate::{models::TokenInfo, providers::TokenChainData};

fn format_raw_amount(raw: &str, decimals: u8) -> String {
    if decimals == 0 { return raw.to_owned(); }
    let decimals = decimals as usize;
    let digits = raw.trim_start_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let padded = if digits.len() <= decimals {
        format!("{}{}", "0".repeat(decimals + 1 - digits.len()), digits)
    } else { digits.to_owned() };
    let split = padded.len() - decimals;
    let whole = &padded[..split];
    let fraction = padded[split..].trim_end_matches('0');
    if fraction.is_empty() { whole.to_owned() } else { format!("{whole}.{fraction}") }
}

pub fn from_chain(chain: &TokenChainData) -> TokenInfo {
    TokenInfo {
        decimals: Some(chain.decimals),
        total_supply: Some(format_raw_amount(&chain.total_supply, chain.decimals)),
        circulating_supply: None,
        token_standard: Some(match chain.token_program.as_str() {
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" => "SPL Token".to_owned(),
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb" => "Token-2022".to_owned(),
            _ => "Unknown token program".to_owned(),
        }),
        creator_address: chain.creator_address.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::format_raw_amount;
    #[test]
    fn formats_supply_without_floating_point_rounding() {
        assert_eq!(format_raw_amount("1234567", 6), "1.234567");
        assert_eq!(format_raw_amount("1000000", 6), "1");
        assert_eq!(format_raw_amount("42", 6), "0.000042");
    }
}
