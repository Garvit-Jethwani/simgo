# Security policy

## Reporting a vulnerability

Please do not open a public issue for security problems. Use GitHub's
private vulnerability reporting on this repository (Security tab, "Report a
vulnerability"), with steps to reproduce and the affected version.

## Scope notes

- The hosted service's workers **run the code they are given** (`go test`
  on the submitted repository). Only submit repositories you trust; the
  worker's IAM role is scoped to its queue, table and bucket prefix.
- The API authenticates with a single bearer token. Treat it like a
  password and rotate it with
  `terraform apply -replace=random_password.api_token`.
