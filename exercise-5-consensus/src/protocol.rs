//! Contains the network-message-types for the consensus protocol and banking application.
use std::collections::HashMap;
use crate::network::Channel;

/// Message-type of the network protocol.
#[derive(Debug)]
pub enum Command {
	/// Open an account with a unique name.
	Open { account: String },
	
	/// Deposit money into an account.
	Deposit { account: String, amount: usize },
	
	/// Withdraw money from an account.
	Withdraw { account: String, amount: usize },
	
	/// Transfer money between accounts.
	Transfer { src: String, dst: String, amount: usize },
	
	/// Accept a new network connection.
	Accept(Channel<Command>),
	
	// TODO: add other useful control messages

	/// Checks the time since the last contact to the leader
	CheckForTimeout {},

	/// After a timeout wait for another 1-299 ms before starting an election
	Timeout {},

	/// Start an election
	Election{},
	/// Call to vote
	RequestVote { candidate_id: usize, candidate_term: usize, last_log_term: usize, last_log_index: usize },
	/// Accept candidate
	VoteYes { voter_id: usize },
	/// Reject candidate
	VoteNo { voter_id: usize, voter_term: usize },

	/// Periodically sending heartbeats
	SendingHeartbeat{},

	/// Heartbeat -> TODO add payload
	AppendEntry { leader_term: usize, leader_id: usize, leader_commit: usize, last_entry: LogEntry, current_entry: LogEntry },
	AppendEntryResponse { success: bool, term: usize, responder_id: usize, responder_index: usize },

	/// Forward Command
	ForwardedCommand { forwarded: LogEntry, origin_id: usize },

	//ResultForwarding { success: bool },
	//ReceiveCommand { forwarded: LogEntry },

	/// Empty Command for Heartbeat
	HeartBeat {}

}

// TODO: add other useful structures and implementations
#[derive(Debug)]
#[derive(PartialEq)]
pub enum State {
	Leader,
	Candidate,
	Follower
}
#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq)]
pub enum Transaction {
	Heartbeat,
	Open,
	Deposit,
	Transfer,
	Withdraw
}
#[derive(Debug)]
#[derive(Clone)]
pub struct LogEntry {
	// Entry
	pub command_type: Transaction,
	pub acc1: String,
	pub acc2: String,
	pub amount: usize,
	// term in which it was appended to the log
	pub term: usize,
	// Identification for the entry
	pub origin_id: usize,
	pub origin_nr: usize,
}

pub fn compare_log_entries(entry_1: &LogEntry, entry_2: &LogEntry) -> bool{
	if (entry_1.origin_id == entry_2.origin_id) & (entry_1.origin_nr == entry_2.origin_nr) {
		return true;
	}
	return false;
}
pub fn commit(entry: LogEntry, mut bank_db: &mut HashMap<String, usize>) -> String{
	match entry.command_type {
		Transaction::Open => {
			if bank_db.contains_key(&entry.acc1){
				return format!("Err[_=>{}]: Das Konto '{}' existiert bereits.", entry.acc1.clone(), entry.acc1.clone());
			} else {
				bank_db.insert(entry.acc1.clone(), 0);
				return format!("Ok[_=>{}]: Das Konto '{}' wurde erstellt.", entry.acc1.clone(), entry.acc1);
			}
		},
		Transaction::Deposit => {
			if !bank_db.contains_key(&entry.acc1){
				return format!("Err[{}=>+{}€]: Das Konto '{}' existiert nicht.", entry.acc1.clone(), entry.amount, entry.acc1.clone());
			} else {
				*bank_db.get_mut(&entry.acc1).unwrap() += entry.amount;
				println!("Neuer Wert: {}",bank_db.get(&entry.acc1).unwrap());
				return format!("Ok[{}=>+{}€]: Dem Konto '{}' wurden {}€ gutgeschrieben.", entry.acc1.clone(), entry.amount, entry.acc1.clone(), entry.amount);
			}
		},
		Transaction::Transfer => {
			if !bank_db.contains_key(&entry.acc1){
				return format!("Err[{}={}€=>{}]: Das Konto '{}' existiert nicht.", entry.acc1.clone(), entry.amount, entry.acc2.clone(), entry.acc1.clone());
			} else if !bank_db.contains_key(&entry.acc2){
				return format!("Err[{}={}€=>{}]: Das Konto '{}' existiert nicht.", entry.acc1.clone(), entry.amount, entry.acc2.clone(), entry.acc2.clone());
			} else if bank_db.get(&entry.acc1).unwrap() < &entry.amount {
				return format!("Err[{}={}€=>{}]: Das Konto '{}' enthält keine {}€.", entry.acc1.clone(), entry.amount, entry.acc2.clone(), entry.acc1.clone(), entry.amount);
			} else {
				*bank_db.get_mut(&entry.acc1).unwrap() -= entry.amount;
				*bank_db.get_mut(&entry.acc2).unwrap() += entry.amount;
				return format!("Ok[{}={}€=>{}]: Dem Konto '{}' wurden {}€ vom Konto '{}' überwiesen.", entry.acc1.clone(), entry.amount, entry.acc2.clone(), entry.acc2.clone(), entry.amount, entry.acc1.clone());
			}
		},
		Transaction::Withdraw => {
			if !bank_db.contains_key(&entry.acc1){
				return format!("Err[{}=>-{}€]: Das Konto '{}' existiert nicht.", entry.acc1.clone(), entry.amount, entry.acc1.clone());
			} else if bank_db.get(&entry.acc1).unwrap() < &entry.amount {
				return format!("Err[{}=>-{}€]: Das Konto '{}' enthält keine {}€.", entry.acc1.clone(), entry.amount, entry.acc1.clone(), entry.amount);
			} else {
				let current_value = bank_db.get(&entry.acc1).unwrap() - entry.amount;
				*bank_db.get_mut(&entry.acc1).unwrap() -= entry.amount;
				return format!("Ok[{}=>-{}€]: Vom Konto '{}' wurden {}€ abgehoben.", entry.acc1.clone(), entry.amount, entry.acc1.clone(), entry.amount);
			}
		},
		_ => {}
	}
	return  "".to_string();
}

/// Helper macro for defining test-scenarios.
/// 
/// The basic idea is to write test-cases and then observe the behavior of the
/// simulated network through the tracing mechanism for debugging purposes.
/// 
/// The macro defines a mini-language to easily express sequences of commands
/// which are executed concurrently unless you explicitly pass time between them.
/// The script needs some collection of channels to operate over which has to be
/// provided as the first command (see next section).
/// Commands are separated by semicolons and are either requests (open an
/// account, deposit money, withdraw money and transfer money between accounts)
/// or other commands (currently only sleep).
/// 
/// # Examples
/// 
/// The following script creates two accounts (Foo and Bar) in different branch
/// offices, deposits money in the Foo-account, waits a second, transfers it to
/// bar, waits another half second and withdraws the money. The waiting periods
/// are critical, because we need to give the consensus-protocol time to confirm
/// the sequence of transactions before referring to changes made in a different
/// branch office. Within one branch office the timing is not important since
/// the commands are always delivered in sequence.
/// 
/// ```rust
///     let channels: Vec<Channel<_>>;
///     script! {
///         use channels;
///         [0] "Foo" => open(), deposit(10);
///         [1] "Bar" => open();
///         sleep();   // the argument defaults to 1 second
///         [0] "Foo" => transfer("Bar", 10);
///         sleep(0.5);// may also sleep for fractions of a second
///         [1] "Bar" => withdraw(10);
///     }
/// ```
#[macro_export]
macro_rules! script {
	// empty base case
	(@expand $chan_vec:ident .) => {};
	
	// meta-rule for customer requests
	(@expand $chan_vec:ident . [$id:expr] $acc:expr => $($cmd:ident($($arg:expr),*)),+; $($tail:tt)*) => {
		$(
			$chan_vec[$id].send(
				script! { @request $cmd($acc, $($arg),*) }
			);
		)*
		script! { @expand $chan_vec . $($tail)* }
	};
	
	// meta-rule for other commands
	(@expand $chan_vec:ident . $cmd:ident($($arg:expr),*); $($tail:tt)*) => {
		script! { @command $cmd($($arg),*) }
		script! { @expand $chan_vec . $($tail)* }
	};
	
	// customer requests
	(@request open($holder:expr,)) => {
		$crate::protocol::Command::Open {
			account: $holder.into()
		}
	};
	(@request deposit($holder:expr, $amount:expr)) => {
		$crate::protocol::Command::Deposit {
			account: $holder.into(),
			amount: $amount
		}
	};
	(@request withdraw($holder:expr, $amount:expr)) => {
		$crate::protocol::Command::Withdraw {
			account: $holder.into(),
			amount: $amount
		}
	};
	(@request transfer($src:expr, $dst:expr, $amount:expr)) => {
		$crate::protocol::Command::Transfer {
			src: $src.into(),
			dst: $dst.into(),
			amount: $amount
		}
	};
	
	// other commands
	(@command sleep($time:expr)) => {
		std::thread::sleep(std::time::Duration::from_millis(($time as f64 * 1000.0) as u64));
	};
	(@command sleep()) => {
		std::thread::sleep(std::time::Duration::from_millis(1000));
	};
	
	// entry point for the user
	(use $chan_vec:expr; $($tail:tt)*) => {
		let ref channels = $chan_vec;
		script! { @expand channels . $($tail)* }	
	};
	
	// rudimentary error diagnostics
	(@request $cmd:ident $($tail:tt)*) => {
		compile_error!("maybe you mean one of open, deposit, withdraw or transfer?")
	};
	(@command $cmd:ident $($tail:tt)*) => {
		compile_error!("maybe you mean sleep or forgot the branch index?")
	};
	(@expand $($tail:tt)*) => {
		compile_error!("illegal command syntax")
	};
	($($tail:tt)*) => {
		compile_error!("missing initial 'use <channels>;'")
	};
}
