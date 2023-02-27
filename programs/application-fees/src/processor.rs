use {
    crate::instruction::ApplicationFeesInstuctions,
    solana_program_runtime::{ic_msg, invoke_context::InvokeContext},
    solana_sdk::{
        instruction::InstructionError, program_utils::limited_deserialize,
        transaction_context::IndexOfAccount,
    },
    std::cmp::min,
};

pub fn process_instruction(
    _first_instruction_account: IndexOfAccount,
    invoke_context: &mut InvokeContext,
) -> Result<(), InstructionError> {
    let transaction_context = &invoke_context.transaction_context;
    let instruction_context = transaction_context.get_current_instruction_context()?;
    let instruction_data = instruction_context.get_instruction_data();

    match limited_deserialize(instruction_data)? {
        ApplicationFeesInstuctions::CheckFees { minimum_fees } => {
            Processor::check_application_fees(invoke_context, minimum_fees)
        }
        ApplicationFeesInstuctions::Rebate { rebate_fees } => {
            Processor::rebate(invoke_context, rebate_fees)
        }
        ApplicationFeesInstuctions::PayApplicationFees { .. } => {
            // Pay application fees is deserialized by runtime, may be tried to cpi here which should not work
            Err(InstructionError::InvalidError)
        }
    }
}

pub struct Processor;

impl Processor {
    fn check_application_fees(
        invoke_context: &mut InvokeContext,
        minimum_fees: u64,
    ) -> Result<(), InstructionError> {
        let transaction_context = &invoke_context.transaction_context;
        let instruction_context = transaction_context.get_current_instruction_context()?;

        let account = instruction_context.try_borrow_instruction_account(transaction_context, 0)?;

        let account_key = *account.get_key();
        ic_msg!(
            invoke_context,
            "ApplicationFeesInstuctions::CheckFees called for {} for minimum amount {}",
            account_key.to_string(),
            minimum_fees
        );
        drop(account);
        if let Some(fees) = invoke_context
            .application_fees
            .application_fees
            .get(&account_key)
        {
            if *fees >= minimum_fees {
                Ok(())
            } else {
                Err(InstructionError::ApplicationFeesInsufficient(
                    account_key,
                    minimum_fees,
                ))
            }
        } else {
            Err(InstructionError::ApplicationFeesInsufficient(
                account_key,
                minimum_fees,
            ))
        }
    }

    fn rebate(
        invoke_context: &mut InvokeContext,
        rebate_fees: u64,
    ) -> Result<(), InstructionError> {
        let transaction_context = &invoke_context.transaction_context;
        let instruction_context = transaction_context.get_current_instruction_context()?;

        let owner = instruction_context.try_borrow_instruction_account(transaction_context, 1)?;
        if !owner.is_signer() {
            ic_msg!(invoke_context, "Authority account must be a signer");
            return Err(InstructionError::MissingRequiredSignature);
        }

        let index_in_transaction =
            instruction_context.get_index_of_instruction_account_in_transaction(0)?;
        let account_key = transaction_context.get_key_of_account_at_index(index_in_transaction)?;
        let account = {
            if account_key.eq(owner.get_key()) {
                // account is owner of itself usually case for most of the accounts and pdas.
                // check if owner is owner of itself
                if !owner.get_owner().eq(owner.get_key()) {
                    ic_msg!(
                        invoke_context,
                        "Invalid account owner {} instead of {}",
                        owner.get_owner().to_string(),
                        owner.get_key().to_string()
                    );
                    return Err(InstructionError::IllegalOwner);
                }

                owner
            } else {
                let account =
                    instruction_context.try_borrow_instruction_account(transaction_context, 0)?;

                if !account.get_owner().eq(owner.get_key()) {
                    ic_msg!(
                        invoke_context,
                        "Invalid account owner {} instead of {}",
                        account.get_owner().to_string(),
                        owner.get_key().to_string()
                    );
                    return Err(InstructionError::IllegalOwner);
                }

                drop(owner);
                account
            }
        };
        drop(account);
        // do rebate if there is an application fee
        // the maximum rebate amount is application fees amount
        let lamports_rebated = {
            let lamports = invoke_context
                .application_fees
                .application_fees
                .get(&account_key);
            if let Some(lamports) = lamports {
                min(*lamports, rebate_fees)
            } else {
                0
            }
        };
        if lamports_rebated > 0 {
            let initial_rebate = invoke_context.application_fees.rebated.get_mut(account_key);

            match initial_rebate {
                Some(initial_rebate) => {
                    // there was already a rebate take, rebate the maximum between them
                    *initial_rebate = std::cmp::max(*initial_rebate, lamports_rebated);
                }
                None => {
                    // no rebate yet
                    invoke_context
                        .application_fees
                        .rebated
                        .insert(*account_key, lamports_rebated);
                }
            }
            // log rebate message
            ic_msg!(
                invoke_context,
                "application fees rebated for writable account {} lamports {}",
                account_key.to_string(),
                lamports_rebated
            );
        }
        Ok(())
    }
}
