Improvements to `fileSystemShould`

- List all backends for which the integration tests were skipped for lack of a proper testing environment
- Do not fail integration tests if no external backend is configured
- run tests on internal backends, even if no external backend is configured