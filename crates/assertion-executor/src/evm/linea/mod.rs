mod evm;
pub(crate) mod opcodes;
mod precompiles;

pub use evm::{
    LineaEvm,
    build_linea_evm,
};
