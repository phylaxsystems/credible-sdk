use anyhow::{
    Result,
    anyhow,
};
use mdbx::{
    StateReader,
    StateWriter,
};
use std::{
    path::{
        Path,
        PathBuf,
    },
    sync::{
        Arc,
        OnceLock,
    },
};

static MDBX_RUNTIME: OnceLock<Arc<MdbxRuntime>> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct MdbxRuntime {
    path: PathBuf,
    writer: StateWriter,
    reader: StateReader,
}

impl MdbxRuntime {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn writer(&self) -> StateWriter {
        self.writer.clone()
    }

    #[must_use]
    pub fn reader(&self) -> StateReader {
        self.reader.clone()
    }
}

fn init_with_cell(
    cell: &OnceLock<Arc<MdbxRuntime>>,
    path: impl AsRef<Path>,
) -> Result<Arc<MdbxRuntime>> {
    let path = path.as_ref().to_path_buf();
    let runtime = if let Some(runtime) = cell.get() {
        runtime.clone()
    } else {
        let writer = StateWriter::new(&path)?;
        let reader = writer.reader().clone();
        let candidate = Arc::new(MdbxRuntime {
            path: path.clone(),
            writer,
            reader,
        });

        match cell.set(Arc::clone(&candidate)) {
            Ok(()) => candidate,
            Err(candidate) => {
                drop(candidate);
                cell.get()
                    .cloned()
                    .ok_or_else(|| anyhow!("shared MDBX runtime initialization race"))?
            }
        }
    };

    if runtime.path() != path.as_path() {
        return Err(anyhow!(
            "shared MDBX runtime already initialized for {}, cannot reuse for {}",
            runtime.path().display(),
            path.display()
        ));
    }

    Ok(runtime)
}

pub fn init(path: impl AsRef<Path>) -> Result<Arc<MdbxRuntime>> {
    init_with_cell(&MDBX_RUNTIME, path)
}

#[cfg(test)]
pub(crate) fn init_test_cell(
    cell: &OnceLock<Arc<MdbxRuntime>>,
    path: impl AsRef<Path>,
) -> Result<Arc<MdbxRuntime>> {
    init_with_cell(cell, path)
}

#[must_use]
pub fn get() -> Option<Arc<MdbxRuntime>> {
    MDBX_RUNTIME.get().cloned()
}
