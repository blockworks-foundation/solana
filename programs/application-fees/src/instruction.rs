use serde::{Deserialize, Serialize};
use solana_frozen_abi_macro::{AbiEnumVisitor, AbiExample};
use solana_program::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};

// application fees instructions
#[derive(AbiExample, AbiEnumVisitor, Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum ApplicationFeesInstuctions {
    // Pay application fees for list of accounts
    PayApplicationFees { fees: Vec<u64> },
    // Check if minimum fees for an account are paid
    CheckFees { minimum_fees: u64 },
    // The account owner i.e program usually can CPI this instruction to rebate the fees to good actors
    // Rebate take fee amount to be rebated
    Rebate { rebate_fees: u64 },
}

pub fn pay_application_fees(accounts: &[Pubkey], fees: Vec<u64>) -> Instruction {
    let accounts = accounts
        .iter()
        .map(|x| AccountMeta::new_readonly(*x, false))
        .collect();
    Instruction::new_with_bincode(
        crate::id(),
        &ApplicationFeesInstuctions::PayApplicationFees { fees },
        accounts,
    )
}

pub fn check_application_fees(account: &Pubkey, fees: u64) -> Instruction {
    Instruction::new_with_bincode(
        crate::id(),
        &ApplicationFeesInstuctions::CheckFees { minimum_fees: fees },
        vec![AccountMeta::new_readonly(*account, false)],
    )
}

pub fn rebate(account: &Pubkey, owner: &Pubkey, rebate_fees: u64) -> Instruction {
    Instruction::new_with_bincode(
        crate::id(),
        &ApplicationFeesInstuctions::Rebate { rebate_fees },
        vec![
            AccountMeta::new_readonly(*account, false),
            AccountMeta::new_readonly(*owner, true),
        ],
    )
}

pub fn rebate_pda(account: &Pubkey, rebate_fees: u64) -> Instruction {
    Instruction::new_with_bincode(
        crate::id(),
        &ApplicationFeesInstuctions::Rebate { rebate_fees },
        vec![
            AccountMeta::new_readonly(*account, false),
            AccountMeta::new_readonly(*account, true),
        ],
    )
}
