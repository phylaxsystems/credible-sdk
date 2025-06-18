pub mod args;
pub mod utils;

#[derive(Clone)]
pub struct Assertion {
    file_name: Option<String>,
    contract_name: String,
}

impl Assertion {
    const SUPPORTED_EXTENSIONS: &'static [&'static str] = &[".a.sol", ".sol"];

    pub fn new(file_name: Option<String>, contract_name: String) -> Self {
        Self {
            file_name,
            contract_name,
        }
    }

    pub fn get_paths(&self) -> Vec<String> {
        match &self.file_name {
            Some(file_name) => vec![file_name.clone()],
            None => {
                let mut file_names = Vec::new();
                for ext in Self::SUPPORTED_EXTENSIONS {
                    let path = format!("{}{}", self.contract_name, ext);
                    file_names.push(path);
                }
                file_names
            }
        }
    }

    pub fn contract_name(&self) -> &String {
        &self.contract_name
    }
}
