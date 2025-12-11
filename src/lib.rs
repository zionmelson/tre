pub mod adapter_book;
pub mod bootstrap;
pub mod builder;
pub mod bundle;
pub mod config;
pub mod core;
pub mod corecast;
pub mod notifier;
pub mod planner;
pub mod protocol;
pub mod proto;
pub mod runtime;
pub mod strategy;
pub mod submission;
pub mod template;
pub mod telemetry;
pub mod tip_pressure;
pub mod token_pair;
pub mod watch;

// Module surface restored so downstream crates can keep referencing the
// surviving planner/builder/submission components during the CoreCast-first
// cleanup.
