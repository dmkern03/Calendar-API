# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.x.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability, please report it by emailing the maintainer directly rather than opening a public issue.

Please include:

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

You can expect an initial response within 48 hours. We will work with you to understand and address the issue promptly.

## Security Best Practices

When using this project:

1. **API Keys**: Store FRED API keys securely using Databricks Secrets or environment variables. Never commit API keys to version control.

2. **Access Control**: Use appropriate Databricks workspace permissions and Unity Catalog grants to control access to the calendar dimension table.

3. **Dependencies**: Regularly update dependencies to patch known vulnerabilities.
