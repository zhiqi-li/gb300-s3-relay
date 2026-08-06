# Security policy

## Reporting a vulnerability

Do not open a public issue for a vulnerability that could expose credentials, inference
content, or deployment infrastructure. Contact the repository owner privately through the
security reporting channel configured on GitHub.

## Deployment guidance

- Keep S3 credentials and model-server keys outside source control.
- Reference secrets through credential files or environment-variable names; never place secret
  values in TOML files or command-line arguments.
- Bind the gateway to loopback unless `auth_token_env` is configured.
- Use a dedicated bucket prefix and credentials with only the required object permissions.
- Treat request payloads, generated output, worker logs, and dead letters as sensitive data.
- Configure bucket lifecycle policies to remove objects after the required retention period.
- Rotate credentials immediately if a scanner or operator detects accidental disclosure.

See [`AGENT_INTEGRATION.md`](AGENT_INTEGRATION.md) for the deployment contract.
