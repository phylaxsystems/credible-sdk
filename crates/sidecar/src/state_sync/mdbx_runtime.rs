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

pub fn init(path: impl AsRef<Path>) -> Result<Arc<MdbxRuntime>> {
    let path = path.as_ref().to_path_buf();
    let runtime = if let Some(runtime) = MDBX_RUNTIME.get() {
        runtime.clone()
    } else {
        let writer = StateWriter::new(&path)?;
        let reader = writer.reader().clone();
        let candidate = Arc::new(MdbxRuntime {
            path: path.clone(),
            writer,
            reader,
        });

        match MDBX_RUNTIME.set(Arc::clone(&candidate)) {
            Ok(()) => candidate,
            Err(candidate) => {
                drop(candidate);
                MDBX_RUNTIME
                    .get()
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

#[must_use]
pub fn get() -> Option<Arc<MdbxRuntime>> {
    MDBX_RUNTIME.get().cloned()
}
