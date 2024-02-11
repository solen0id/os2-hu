use crate::protocol::Command;
use std::collections::HashMap;
use std::error::Error;

pub struct Bank {
    accounts: HashMap<String, usize>,
}

impl Bank {
    pub fn new() -> Bank {
        Bank {
            accounts: HashMap::new(),
        }
    }

    pub fn apply_command(&mut self, cmd: &Command) -> Result<String, Box<dyn Error>> {
        match cmd {
            Command::Open { account } => {
                self.open_account(account.to_string());
                Ok("Account opened".into())
            }

            Command::Deposit { account, amount } => {
                let balance = self.deposit(account.to_string(), *amount)?;
                Ok(format!("Deposited {}, balance is {}", amount, balance))
            }

            Command::Withdraw { account, amount } => {
                let balance = self.withdraw(account.to_string(), *amount)?;
                Ok(format!("Withdrew {}, balance is {}", amount, balance))
            }

            Command::Transfer { src, dst, amount } => {
                let (src_balance, dst_balance) =
                    self.transfer(src.clone(), dst.clone(), *amount)?;
                Ok(format!(
                    "Transferred {} from {} to {}, balances are {} and {}",
                    amount,
                    src.clone(),
                    dst.clone(),
                    src_balance,
                    dst_balance
                ))
            }

            _ => Err("Unknown command".into()),
        }
    }

    pub fn open_account(&mut self, account: String) {
        self.accounts.insert(account, 0);
    }

    pub fn deposit(&mut self, account: String, amount: usize) -> Result<usize, Box<dyn Error>> {
        if !self.accounts.contains_key(&account) {
            return Err("Can not deposit, Account not found".into());
        }

        let balance = self.accounts.entry(account).or_insert(0);
        *balance += amount;

        Ok(*balance)
    }

    pub fn withdraw(&mut self, account: String, amount: usize) -> Result<usize, Box<dyn Error>> {
        if !self.accounts.contains_key(&account) {
            return Err("Can not withdraw, Account not found".into());
        }

        let balance = self.accounts.entry(account).or_insert(0);
        *balance -= amount;

        Ok(*balance)
    }

    pub fn transfer(
        &mut self,
        src: String,
        dst: String,
        amount: usize,
    ) -> Result<(usize, usize), Box<dyn Error>> {
        if !self.accounts.contains_key(&src) || !self.accounts.contains_key(&dst) {
            return Err("Can not transfer, Account not found".into());
        }

        {
            let src_balance = self.accounts.entry(src.clone()).or_insert(0);
            *src_balance -= amount;
        }

        {
            let dst_balance = self.accounts.entry(dst.clone()).or_insert(0);
            *dst_balance += amount;
        }

        Ok((
            *self.accounts.get(&src).unwrap(),
            *self.accounts.get(&dst).unwrap(),
        ))
    }
}
