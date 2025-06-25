# executor fuzz tests

This folder contains fuzz tests for the assertion executor. The available tests can be viewed in `./fuzz_targets`.

To run a fuzz test, use the following command:

```bash
cargo fuzz run logs_fuzz -- -max_len=200
```
From the top level assertion executor folder.